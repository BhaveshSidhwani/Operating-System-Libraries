/*
 *  Copyright (C) 2023 CS416 Rutgers CS
 *	Tiny File System
 *	File:	rufs.c
 *	Group members: 
 *		Bhavesh Veersen Sidhwani (bs1061)
 *		Shobhit Singh (ss4363)
 *		iLab used: cp.cs.rutgers.edu
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#include <limits.h>

#include "block.h"
#include "rufs.h"

#define TRUE 1
#define FALSE 0

char diskfile_path[PATH_MAX];

// Declare your in-memory data structures here

int is_fs_init = -1;	// Flag to check if File system is initialized

int inode_per_blk;		// Number of inodes per block
int dirent_per_blk;		// Number of dirents per block


/* Super block information */
struct superblock* super_block = NULL;

int super_blk_key = 0;	// Super Block
int i_bm_blk_key = 1;	// Inode Bitmap Block
int d_bm_blk_key = 2;	// Data Bitmap Block
int i_blk_key = 3;		// Inode Block Start
int d_blk_key;			// Data Block Start



void get_used_blocks_count() {
	bitmap_t i_bitmap = malloc(BLOCK_SIZE);
	bitmap_t d_bitmap = malloc(BLOCK_SIZE);

	if (bio_read(i_bm_blk_key, i_bitmap) < 0) {
		printf("Error in bio read\n");
		return;
	}
	if (bio_read(d_bm_blk_key, d_bitmap) < 0) {
		printf("Error in bio read\n");
		return;
	}

	int i_count = 0;
	int d_count = 0;
	for (int i=0; i<MAX_INUM; i++) {
		if (get_bitmap(i_bitmap, i) == 1)
			i_count++;
	}
	for (int i=0; i<MAX_DNUM; i++) {
		if (get_bitmap(d_bitmap, i) == 1)
			d_count++;
	}
	int i_blk_count = (i_count/16) + 1;
	printf("Super Block Count: 1\n");
	printf("Bitmap Block Count: 2\n");
	printf("Inode Block Count: %d\n", i_blk_count);
	printf("Data Block Count: %d\n", d_count);
	printf("Total Block Count: %d\n", (1 + 2 + i_blk_count + d_count));
}


/* 
 * Get available inode number from bitmap
 */
int get_avail_ino() {
	printf("get_avail_ino...\n");

	// Step 1: Read inode bitmap from disk
	bitmap_t i_bitmap = malloc(BLOCK_SIZE);
	if (bio_read(i_bm_blk_key, i_bitmap) < 0){
		return -1;
	}
	// Step 2: Traverse inode bitmap to find an available slot
	for (int i=0; i<super_block->max_inum; i++) {
		if (get_bitmap(i_bitmap, i) == 0) {
			set_bitmap(i_bitmap, i);
			bio_write(i_bm_blk_key, i_bitmap);

			free(i_bitmap);
			return i;
		}
	}

	// Step 3: Update inode bitmap and write to disk 
	free(i_bitmap);
	return -1;
}

/* 
 * Get available data block number from bitmap
 */
int get_avail_blkno() {
	printf("get_avail_blkno...\n");

	// Step 1: Read data block bitmap from disk
	bitmap_t d_bitmap = malloc(BLOCK_SIZE);
	if (bio_read(d_bm_blk_key, d_bitmap) < 0){
		return -1;
	}
	
	// Step 2: Traverse data block bitmap to find an available slot
	for (int i=0; i<super_block->max_dnum; i++) {
		if (get_bitmap(d_bitmap, i) == 0) {
			set_bitmap(d_bitmap, i);
			bio_write(d_bm_blk_key, d_bitmap);

			free(d_bitmap);
			return i;
		}
	}

	// Step 3: Update data block bitmap and write to disk 
	free(d_bitmap);
	return -1;
}

/* 
 * inode operations
 */
int readi(uint16_t ino, struct inode *inode) {
	printf("readi...\n");

	// Step 1: Get the inode's on-disk block number
	int blk_key = ino / inode_per_blk;
	blk_key += super_block->i_start_blk;	// Number of dirents per blockk;

	// Step 2: Get offset of the inode in the inode on-disk block
	int offset = ino % inode_per_blk;
	offset *= sizeof(struct inode);			// Number of dirents per block

	// Step 3: Read the block from disk and then copy into inode structure
	void* inode_blk = malloc(BLOCK_SIZE);
	if (bio_read(blk_key, inode_blk) < 0) {
		return -1;
	}
	void* src_inode = inode_blk + offset;
	memcpy(inode, src_inode, sizeof(struct inode));

	return 0;
}

int writei(uint16_t ino, struct inode *inode) {
	printf("writei...\n");

	// Step 1: Get the block number where this inode resides on disk
	int blk_key = ino / inode_per_blk;
	blk_key += super_block->i_start_blk;	// Number of dirents per blockk;
	
	// Step 2: Get the offset in the block where this inode resides on disk
	int offset = ino % inode_per_blk;
	offset *= sizeof(struct inode);			// Number of dirents per block

	// Step 3: Write inode to disk 
	void* i_blk = malloc(BLOCK_SIZE);
	if (bio_read(blk_key, i_blk) < 0) {
		return -1;
	}
	void* dest_inode = i_blk + offset;
	memcpy(dest_inode, inode, sizeof(struct inode));
	if (bio_write(blk_key, i_blk) < 0) {
		return -1;
	}

	return 0;
}


/* 
 * directory operations
 */
int dir_find(uint16_t ino, const char *fname, size_t name_len, struct dirent *dirent) {
	printf("dir_find...\n");

	// Step 1: Call readi() to get the inode using ino (inode number of current directory)
	printf("ino: %d..\n", ino);
	printf("fname: %s ...\n", fname);
	struct inode* curr_dir_inode = (struct inode*) malloc(sizeof(struct inode));
	if (readi(ino, curr_dir_inode) < 0) {
		printf("Error in readi...\n");
		return -1;
	}
	
	struct dirent* temp_d_blk = malloc(BLOCK_SIZE);
	for (int i=0; i<16; i++) {
		if (curr_dir_inode->direct_ptr[i] == -1)
			continue;

		int blk_num = super_block->d_start_blk + curr_dir_inode->direct_ptr[i];
		// temp_d_blk = malloc(BLOCK_SIZE);
		// Step 2: Get data block of current directory from inode
		if (bio_read(blk_num, (void*) temp_d_blk) < 0) {
			printf("Error in bio_read...\n");
			return -1;
		}
		
		// Step 3: Read directory's data block and check each directory entry.
		//If the name matches, then copy directory entry to dirent structure
		for (int j=0; j<dirent_per_blk; j++) {
			if (temp_d_blk[j].valid == 0)
				continue;

			if(strcmp(temp_d_blk[j].name, fname) == 0) {
				memcpy(dirent, &temp_d_blk[j], sizeof(struct dirent));
				free(curr_dir_inode);
				free(temp_d_blk);
				return 0;
			}
		}
	}
	
	free(curr_dir_inode);
	if (temp_d_blk != NULL) {
		free(temp_d_blk);
	}
	return -1;
}

int dir_add(struct inode dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and check each directory entry of dir_inode
	
	// Step 2: Check if fname (directory name) is already used in other entries

	// Step 3: Add directory entry in dir_inode's data block and write to disk

	// Allocate a new data block for this directory if it does not exist

	// Update directory inode

	// Write directory entry

	int* dir_inode_data = malloc(16*sizeof(int));
	memcpy(dir_inode_data, dir_inode.direct_ptr, 16*sizeof(int));


	int data_blk_index=0;
	
	struct dirent* data_blk = malloc(sizeof(struct dirent));
	int dirent_index = 0;
	char found_empty_slot = 0;
	int empty_unallocated_blk = -1;
	int empty_blk_index = 0;
	int empty_dirent_index = 0;

	bitmap_t data_bitmap = malloc(BLOCK_SIZE);
	if (bio_read(d_bm_blk_key, data_bitmap) < 0) {
		printf("Error in bio_read\n");
		return -1;
	}
	
	for (data_blk_index=0; data_blk_index<16; data_blk_index++) {
		if(dir_inode_data[data_blk_index] == -1){
			if(empty_unallocated_blk == -1){
				empty_unallocated_blk = data_blk_index;
			}
			continue;
		}
		
		bio_read(super_block->d_start_blk+dir_inode_data[data_blk_index], data_blk);
		
		for(dirent_index=0; dirent_index<dirent_per_blk; dirent_index++){
			struct dirent* dir_entry=data_blk+dirent_index;
			if(dir_entry==NULL || dir_entry->valid==0){

				if(found_empty_slot==0){
					found_empty_slot=1;
					empty_blk_index=data_blk_index;
					empty_dirent_index=dirent_index;
				}
				continue;
			}

			if(strcmp(dir_entry->name, fname)==0){
				printf("Duplicate name: %s. Unable to create\n",fname);
				return -1;
			}
		}
	}

	
	struct dirent* new_entry = malloc(sizeof(struct dirent));
	new_entry->valid = 1;
	new_entry->ino = f_ino;
	strncpy(new_entry->name, fname, name_len+1);

	if (found_empty_slot == 1) {
		bio_read(super_block->d_start_blk+dir_inode_data[empty_blk_index],data_blk);
		data_blk[empty_dirent_index]=*new_entry;
		bio_write(super_block->d_start_blk+dir_inode_data[empty_blk_index],data_blk);
		free(new_entry);
	}
	
	else if (empty_unallocated_blk>-1) {
		if (dir_inode_data[empty_unallocated_blk] == -1) { 
			int block_num = get_avail_blkno();
			dir_inode_data[empty_unallocated_blk] = block_num;
			struct dirent* new_data_blk=malloc(BLOCK_SIZE);
			new_data_blk[0]=*new_entry;
			bio_write(super_block->d_start_blk+block_num,new_data_blk);
			free(new_data_blk);
			free(new_entry);
		}
		else{
			free(new_entry);
		}
	}
	else {
		bitmap_t inode_bitmap=malloc(BLOCK_SIZE);
		bio_read(i_bm_blk_key, inode_bitmap);
		unset_bitmap(inode_bitmap,f_ino);
		bio_write(i_bm_blk_key, inode_bitmap);
		free(inode_bitmap);
		free(data_bitmap);
		free(data_blk);
		free(dir_inode_data);
		return -1;
	}

	int parent_ino=dir_inode.ino;
	struct inode* parent_inode=malloc(sizeof(struct inode));
	readi(parent_ino,parent_inode);
	parent_inode->size+=sizeof(struct dirent);

	memcpy(parent_inode->direct_ptr,dir_inode_data,16*sizeof(int));
	time(& (parent_inode->vstat.st_mtime));
	writei(parent_ino,parent_inode);

	free(parent_inode);
	free(data_bitmap);
	free(data_blk);
	free(dir_inode_data);
	return 0;
}

int dir_remove(struct inode dir_inode, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and checks each directory entry of dir_inode
	
	// Step 2: Check if fname exist

	// Step 3: If exist, then remove it from dir_inode's data block and write to disk

	return 0;
}


int separate_filename(const char* path, char* rem_path, char* filename) {
    char* temp_path = strdup(path);
    char* temp_filename = strrchr(temp_path, '/');
    if (temp_filename == NULL) {
        return -1;
    } else {
		strcpy(filename, temp_filename+1);
        *temp_filename = '\0';
		strcpy(rem_path, temp_path);
    }
	return 0;
}


int get_next_dir_from_path(char* dir_path, char* next_dir, char* rem_path) {
	
	if (dir_path[0] == '/') {
        dir_path += 1;
    }

	char* temp_path = strdup(dir_path);
    char* next_slash = strchr(temp_path, '/');

    if (next_slash == NULL) {
        strcpy(rem_path, dir_path);
        strcpy(next_dir, dir_path);
		free(temp_path);
		return 1;
    } else {
        int len = next_slash - temp_path;

        strncpy(next_dir, temp_path, len);
        next_dir[len] = '\0';
        strcpy(rem_path, next_slash + 1);
    }

    free(temp_path);
	return 0;
}

/* 
 * namei operation
 */
int get_node_by_path(const char *path, uint16_t ino, struct inode *inode) {
	
	// Step 1: Resolve the path name, walk through path, and finally, find its inode.
	// Note: You could either implement it in a iterative way or recursive way

	if (strcmp(path, "/") == 0 || strlen(path) == 0) {
		readi(0, inode);
		return 0;
	}

	char* dir_path = malloc(strlen(path) + 1);
	char* filename = malloc(strlen(path) + 1);
	char* next_dir = malloc(strlen(path) + 1);
	char* rem_path = malloc(strlen(path) + 1);

	struct dirent* dirent = malloc(sizeof(struct dirent));
	int parent_dir_ino = 0;
	int is_last_dir = FALSE;

	separate_filename(path, dir_path, filename);
	
	if (strlen(dir_path) == 0) {
		is_last_dir = TRUE;
	}

	while (is_last_dir == FALSE) {
		is_last_dir = get_next_dir_from_path(dir_path, next_dir, rem_path);
		if (dir_find(parent_dir_ino, next_dir, strlen(next_dir)+1, dirent) < 0) {
			free(dir_path);
			free(filename);
			free(next_dir);
			free(rem_path);
			free(dirent);
			return -ENOENT;
		}
		parent_dir_ino = dirent->ino;
		strcpy(dir_path, rem_path);
	}

	if (dir_find(parent_dir_ino, filename, strlen(filename)+1, dirent) < 0) {
		free(dir_path);
		free(filename);
		free(next_dir);
		free(rem_path);
		free(dirent);
		return -ENOENT;
	}

	if (readi(dirent->ino, inode) < 0) {
		free(dir_path);
		free(filename);
		free(next_dir);
		free(rem_path);
		free(dirent);
		return -1;
	} 
	free(dir_path);
	free(filename);
	free(next_dir);
	free(rem_path);
	free(dirent);

	return 0;
}

/*
 * Initialize inode
*/
void init_inode(struct inode* inode, int type, int is_root) {
	inode->ino = get_avail_ino();
	inode->valid = 1;
	inode->size = 0;
	inode->type = type;
	inode->link = (type == FILE_TYPE || is_root == 1) ? 1 : 2;

	for (int i=0; i<16; i++)
		inode->direct_ptr[i] = -1;
	for(int i=0;i<8; i++)
		inode->indirect_ptr[i] = -1;
	
	struct stat* temp_stat = malloc(sizeof(struct stat));
	temp_stat->st_ino 	= inode->ino;
	temp_stat->st_mode 	= (type == FILE_TYPE) ? (S_IFREG | 0666) : (S_IFDIR | 0755);
	temp_stat->st_nlink = inode->link;
	temp_stat->st_size 	= inode->size;
	time(&temp_stat->st_mtime);

	inode->vstat = *temp_stat;
}


/* 
 * Make filename system
 */
int rufs_mkfs() {

	// Call dev_init() to initialize (Create) Diskfile
	dev_init(diskfile_path);

	is_fs_init = dev_open(diskfile_path);
	if (is_fs_init == -1) {
		return -1;
	}

	// write superblock information
	d_blk_key = i_blk_key + (MAX_INUM / inode_per_blk);
	super_block = malloc(sizeof(struct superblock));
	super_block->magic_num		= MAGIC_NUM;
	super_block->max_inum		= MAX_INUM;
	super_block->max_dnum 		= MAX_DNUM;
	super_block->i_bitmap_blk 	= i_bm_blk_key;
	super_block->d_bitmap_blk 	= d_bm_blk_key;
	super_block->i_start_blk 	= i_blk_key;
	super_block->d_start_blk 	= d_blk_key;

	if (bio_write(super_blk_key, super_block) < 0) {
		printf("super block bio_write failed...\n");
	}

	// initialize inode bitmap
	bitmap_t i_bitmap = malloc(BLOCK_SIZE);
	for (int i=0; i< BLOCK_SIZE; i++) {
		i_bitmap[i] = 0;
	}
	
	if (bio_write(i_bm_blk_key, i_bitmap) < 0) {
		printf("inode bitmap bio_write failed...\n");
	}

	// initialize data block bitmap
	bitmap_t d_bitmap = malloc(BLOCK_SIZE);
	for (int i=0; i<BLOCK_SIZE; i++) {
		d_bitmap[i] = 0;
	}
	
	if (bio_write(d_bm_blk_key, d_bitmap) < 0) {
		printf("data bitmap bio_write failed...\n");
	}

	// update bitmap information for root directory
	set_bitmap(i_bitmap, 0);

	// update inode for root directory
	struct inode* root_inode = malloc(sizeof(struct inode));
	init_inode(root_inode, DIR_TYPE, TRUE);
	writei(0, root_inode);

	return 0;
}


/* 
 * FUSE filename operations
 */
static void *rufs_init(struct fuse_conn_info *conn) {

	inode_per_blk = BLOCK_SIZE / sizeof(struct inode);
	dirent_per_blk = BLOCK_SIZE / sizeof(struct dirent);

	// Step 1a: If disk filename is not found, call mkfs
	is_fs_init = dev_open(diskfile_path);
	if (is_fs_init != 0) {
		if (rufs_mkfs() != 0){
			printf("rufs_mkfs failed...\n");
			exit(-1);
		}
	}

	// Step 1b: If disk filename is found, just initialize in-memory data structures
	// and read superblock from disk
	super_block = malloc(BLOCK_SIZE);
	bio_read(super_blk_key, super_block);

	return super_block;
}

static void rufs_destroy(void *userdata) {
	get_used_blocks_count();

	// Step 1: De-allocate in-memory data structures
	if (super_block != NULL) {
		free(super_block);
	}

	// Step 2: Close diskfile
	dev_close();

}

static int rufs_getattr(const char *path, struct stat *stbuf) {

	// Step 1: call get_node_by_path() to get inode from path
	struct inode* path_inode = malloc(sizeof(struct inode));
	if (get_node_by_path(path, 0, path_inode) < 0) {
		free(path_inode);
		return -ENOENT;
	}

	// Step 2: fill attribute of filename into stbuf from inode
	stbuf->st_ino	= path_inode->vstat.st_ino;
	stbuf->st_uid	= getuid();
	stbuf->st_gid	= getgid();
	stbuf->st_mode	= path_inode->vstat.st_mode;
	stbuf->st_nlink	= path_inode->vstat.st_nlink;
	stbuf->st_size	= path_inode->vstat.st_size;
	stbuf->st_mtime	= path_inode->vstat.st_mtime;

	free(path_inode);

	return 0;
}

static int rufs_opendir(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path
	struct inode* path_inode = malloc(sizeof(struct inode));
	if (get_node_by_path(path, 0, path_inode) < 0) {
		free(path_inode);
		return -ENOENT;
	}
	free(path_inode);

	// Step 2: If not find, return -1

	return 0;
}


static int rufs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: Read directory entries from its data blocks, and copy them to filler

	struct inode* inode = malloc(sizeof(struct inode));
	int found_node = get_node_by_path(path,0,inode);
	// Step 2: If not find, return -1
	if (found_node < 0) {
		free(inode);
		return -1;
	}
	filler(buffer, ".", NULL,offset);
	filler(buffer, "..", NULL,offset);
	// Step 2: Read directory entries from its data blocks, and copy them to filler
	int dir_ptr_index = 0;
	int dir_entry_index = 0;
	struct dirent* data_blk = malloc(BLOCK_SIZE);
	for (dir_ptr_index=0; dir_ptr_index<16; dir_ptr_index++) {
		if (inode->direct_ptr[dir_ptr_index] == -1) {
			continue;
		}

		bio_read(super_block->d_start_blk+inode->direct_ptr[dir_ptr_index], data_blk);
		for(dir_entry_index=0; dir_entry_index<dirent_per_blk; dir_entry_index++){
			if ((data_blk+dir_entry_index)->valid == 1) {
				int status=filler(buffer, (data_blk+dir_entry_index)->name,NULL,offset);
				if(status!=0){
					free(inode);
					free(data_blk);
					return 0;
				}
			}
		}
	}
	free(inode);
	free(data_blk);
	return 0;
}


static int rufs_mkdir(const char *path, mode_t mode) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target directory to parent directory

	// Step 5: Update inode for target directory

	// Step 6: Call writei() to write inode to disk

	char* dir_name = malloc(sizeof(path) + 1);
	char* sub_dir_name = malloc(sizeof(path) + 1);
	separate_filename(path, dir_name, sub_dir_name);

	struct inode* parent_inode = malloc(sizeof(struct inode));
	int found_parent = get_node_by_path(dir_name, 0, parent_inode);
	if(found_parent < 0){
		free(parent_inode);
		free(dir_name);
		free(sub_dir_name);
		return -1;
	}

	struct inode* sub_dir_inode = malloc(sizeof(struct inode));
	init_inode(sub_dir_inode, DIR_TYPE, FALSE);

	int status = dir_add(*parent_inode, sub_dir_inode->ino, sub_dir_name, strlen(sub_dir_name));
	if (status < 0) {
		bitmap_t inode_bitmap=malloc(BLOCK_SIZE);
		bio_read(i_bm_blk_key,inode_bitmap);
		unset_bitmap(inode_bitmap,sub_dir_inode->ino);
		bio_write(i_blk_key,inode_bitmap);
		free(inode_bitmap);
		free(parent_inode);
		free(sub_dir_inode);
		free(dir_name);
		free(sub_dir_name);
		return status;
	}

	writei(sub_dir_inode->ino,sub_dir_inode);

	free(sub_dir_inode);
	free(dir_name);
	free(sub_dir_name);

	return 0;
}


static int rufs_rmdir(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of target directory

	// Step 3: Clear data block bitmap of target directory

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target directory in its parent directory

	return 0;
}

static int rufs_releasedir(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}


static int rufs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target filename name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target filename to parent directory

	// Step 5: Update inode for target filename

	// Step 6: Call writei() to write inode to disk


	char* dir_name = malloc(sizeof(path) +  1);
	char* filename = malloc(sizeof(path) +  1);
	separate_filename(path, dir_name, filename);
	struct inode* parent_inode = malloc(sizeof(struct inode));

	if(get_node_by_path(dir_name, 0, parent_inode) == -1){
		free(dir_name);
		free(filename);
		free(parent_inode);
		return -1;
	}

	struct inode* file_inode = malloc(sizeof(struct inode));
	init_inode(file_inode, FILE_TYPE, FALSE);

	if (dir_add(*parent_inode, file_inode->ino, filename, strlen(filename)) < 0) {
		bitmap_t inode_bitmap = malloc(BLOCK_SIZE);
		bio_read(i_blk_key, inode_bitmap);
		unset_bitmap(inode_bitmap, file_inode->ino);
		bio_write(i_blk_key, inode_bitmap);
		free(dir_name);
		free(filename);
		free(parent_inode);
		free(file_inode);
		return -1;
	}

	writei(file_inode->ino, file_inode);
	free(parent_inode);
	free(file_inode);
	free(dir_name);
	free(filename);
	return 0;
}

static int rufs_open(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path
	struct inode* path_inode = malloc(sizeof(struct inode));
	if (get_node_by_path(path, 0, path_inode) < 0) {
		free(path_inode);
		return -ENOENT;
	}
	free(path_inode);

	// Step 2: If not find, return -1

	return 0;
}


static int rufs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {

	// Step 1: You could call get_node_by_path() to get inode from path
	struct inode* inode = malloc(sizeof(struct inode));
	if (get_node_by_path(path, 0, inode) < 0){
		return -1;
	}

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: copy the correct amount of data from offset to buffer

	// Note: this function should return the amount of bytes you copied to buffer
	
	
	int numBytesRead = 0;
	int numBlockOffset = offset / BLOCK_SIZE;
	int numByteOffset = offset % BLOCK_SIZE;
	int* direct_data_block = malloc(BLOCK_SIZE);
	int* indirect_data_block = malloc(BLOCK_SIZE);
	int direct_ptr_index = 0;
	int indirect_ptr_index = 0;

	char* bufferTail = buffer;
	int* direct_ptr_block = malloc(BLOCK_SIZE);
	if (numBlockOffset < 16) {

		for (direct_ptr_index=numBlockOffset; direct_ptr_index<16; direct_ptr_index++) {
			if (inode->direct_ptr[direct_ptr_index] == -1) {
				continue;
			}
			bio_read(super_block->d_start_blk + inode->direct_ptr[direct_ptr_index], direct_data_block);
			direct_data_block += numByteOffset;
			
			if (BLOCK_SIZE-numByteOffset < size-numBytesRead) {
				memcpy(bufferTail, direct_data_block, BLOCK_SIZE-numByteOffset);
				bufferTail += BLOCK_SIZE-numByteOffset;
				numBytesRead += BLOCK_SIZE-numByteOffset;
			} else {
				memcpy(bufferTail, direct_data_block, size-numBytesRead);
				bufferTail += size-numBytesRead;
				numBytesRead += size-numBytesRead;
				time(& (inode->vstat.st_atime));
				writei(inode->ino, inode);
				free(inode);
				free(direct_data_block);
				free(indirect_data_block);
				free(direct_ptr_block);
				return numBytesRead;
			}
			numByteOffset = 0;
		}

		if (numBytesRead>=size) {
			time(& (inode->vstat.st_atime));
			writei(inode->ino, inode);
			free(inode);
			free(direct_data_block);
			free(indirect_data_block);
			free(direct_ptr_block);
			return numBytesRead;
		}

		for (indirect_ptr_index=0; indirect_ptr_index<8; indirect_ptr_index++) {
			if (inode->indirect_ptr[indirect_ptr_index] == -1) { 
				continue;
			}
			bio_read(super_block->d_start_blk+inode->indirect_ptr[indirect_ptr_index],indirect_data_block);
			for (direct_ptr_index=0; direct_ptr_index<BLOCK_SIZE/sizeof(int); direct_ptr_index++) {
				if (inode->direct_ptr[direct_ptr_index] == -1) {
					continue;
				}
				
				bio_read(super_block->d_start_blk + indirect_data_block[direct_ptr_index], direct_data_block);

				
				if (BLOCK_SIZE-numByteOffset < size-numBytesRead) {
					memcpy(bufferTail, direct_data_block, BLOCK_SIZE-numByteOffset);
					bufferTail += BLOCK_SIZE-numByteOffset;
					numBytesRead += BLOCK_SIZE-numByteOffset;
				}
				else{
					memcpy(bufferTail, direct_data_block, size-numBytesRead);
					bufferTail += size-numBytesRead;
					numBytesRead += size-numBytesRead;
					time(& (inode->vstat.st_atime));
					writei(inode->ino, inode);
					free(inode);
					free(direct_data_block);
					free(indirect_data_block);
					free(direct_ptr_block);
					return numBytesRead;
				}
				numByteOffset = 0;
			}
		}
	} else {
		int indirectOffset = offset - 16*BLOCK_SIZE;
		int numOffsetBlocks = indirectOffset / BLOCK_SIZE;
		int numDirectBlocksPerIndirectPtr = BLOCK_SIZE / sizeof(int);
		int numIndirectOffsetBlocks = numOffsetBlocks / numDirectBlocksPerIndirectPtr;
		int directBlockOffset = numOffsetBlocks % numDirectBlocksPerIndirectPtr;
		numByteOffset = indirectOffset % BLOCK_SIZE;

		for (indirect_ptr_index=numIndirectOffsetBlocks; indirect_ptr_index<8; indirect_ptr_index++) {
			if (inode->indirect_ptr[indirect_ptr_index] == -1) {
				continue;
			}

			bio_read(super_block->d_start_blk + inode->indirect_ptr[indirect_ptr_index], indirect_data_block);
			for(direct_ptr_index=directBlockOffset; direct_ptr_index<numDirectBlocksPerIndirectPtr; direct_ptr_index++){
					if (indirect_data_block[direct_ptr_index] == -1) {
						continue;
					}
			bio_read(super_block->d_start_blk + indirect_data_block[direct_ptr_index], direct_data_block);

			if (BLOCK_SIZE-numByteOffset < size-numBytesRead) {
				memcpy(bufferTail, direct_data_block, BLOCK_SIZE-numByteOffset);
				bufferTail += BLOCK_SIZE-numByteOffset;
				numBytesRead += BLOCK_SIZE-numByteOffset;
			} else {
				memcpy(bufferTail,direct_data_block,size-numBytesRead);
				numBytesRead+=size-numBytesRead;
				time(& (inode->vstat.st_atime));
				writei(inode->ino,inode);
						free(inode);
						free(direct_data_block);
						free(indirect_data_block);
						free(direct_ptr_block);
				return numBytesRead;
			}
			numByteOffset=0;

			}
		}
}

	free(inode);
	free(direct_data_block);
	free(indirect_data_block);
	free(direct_ptr_block);
	return numBytesRead;
}

void initialize_direct_ptr_block(int blockNum){
	int *block_to_initialize = malloc(BLOCK_SIZE);
	bio_read(blockNum, block_to_initialize);
	int i = 0;
	for(i=0; i<BLOCK_SIZE/sizeof(int); i++){
		block_to_initialize[i] = -1;
	}
	bio_write(blockNum, block_to_initialize);
}


static int rufs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: Write the correct amount of data from offset to disk

	// Step 4: Update the inode info and write it to disk

	// Note: this function should return the amount of bytes you write to disk
	struct inode* inode=malloc(BLOCK_SIZE);
	int found_status=get_node_by_path(path,0,inode);

		if(found_status<0){
			return -1;
		}

	int previouslyUnallocated=0;
	int numBytesWritten=0;
	int numBlockOffset=offset/BLOCK_SIZE;
	int numByteOffset=offset%BLOCK_SIZE;
	int* direct_data_block=malloc(BLOCK_SIZE);
	int* indirect_data_block=malloc(BLOCK_SIZE);
	int direct_ptr_index=0;
	int indirect_ptr_index=0;

	char* bufferTail = strdup(buffer);
	int* direct_ptr_block=malloc(BLOCK_SIZE);
	bitmap_t data_bitmap=malloc(BLOCK_SIZE);
	bio_read(d_bm_blk_key, data_bitmap);
	if(numBlockOffset<16){


		for(direct_ptr_index=numBlockOffset;direct_ptr_index<16;direct_ptr_index++){
			if(inode->direct_ptr[direct_ptr_index]!=-1 && get_bitmap(data_bitmap,inode->direct_ptr[direct_ptr_index])==1){

				bio_read(super_block->d_start_blk+inode->direct_ptr[direct_ptr_index],direct_data_block);
				
				if(BLOCK_SIZE-numByteOffset<=size-numBytesWritten){
					memcpy(direct_data_block,bufferTail,BLOCK_SIZE-numByteOffset);

					bio_write(super_block->d_start_blk+inode->direct_ptr[direct_ptr_index],direct_data_block);
					bufferTail+=BLOCK_SIZE-numByteOffset;
					numBytesWritten+=BLOCK_SIZE-numByteOffset;
					inode->size+=BLOCK_SIZE-numByteOffset;

				}
				else if(size-numBytesWritten==0){
					time(& (inode->vstat.st_mtime));
					inode->vstat.st_size += numBytesWritten;
					writei(inode->ino,inode);
					free(inode);
					free(direct_data_block);
					free(indirect_data_block);
					free(direct_ptr_block);
					return numBytesWritten;
				}
				else{
					memcpy(bufferTail,direct_data_block,size-numBytesWritten);
					bio_write(super_block->d_start_blk+inode->direct_ptr[direct_ptr_index],direct_data_block);
					inode->size+=size-numBytesWritten;
					numBytesWritten+=size-numBytesWritten;
					time(& (inode->vstat.st_mtime));
					inode->vstat.st_size += numBytesWritten;
					writei(inode->ino,inode);
					free(inode);
					free(direct_data_block);
					free(indirect_data_block);

					free(direct_ptr_block);
					return numBytesWritten;
				}
				numByteOffset=0;
		} else{
		int new_data_block=get_avail_blkno();
		inode->direct_ptr[direct_ptr_index]=new_data_block;


		bio_read(super_block->d_start_blk+inode->direct_ptr[direct_ptr_index],direct_data_block);


		if(BLOCK_SIZE-numByteOffset<=size-numBytesWritten){
			memcpy(direct_data_block,bufferTail,BLOCK_SIZE-numByteOffset);
			bio_write(super_block->d_start_blk+inode->direct_ptr[direct_ptr_index],direct_data_block);
			inode->size+=BLOCK_SIZE-numByteOffset;
			bufferTail+=BLOCK_SIZE-numByteOffset;
			numBytesWritten+=BLOCK_SIZE-numByteOffset;
		}
		else if(size-numBytesWritten==0){
			time(& (inode->vstat.st_mtime));
			inode->vstat.st_size += numBytesWritten;
			writei(inode->ino,inode);
			free(inode);
			free(direct_data_block);
			free(indirect_data_block);
			free(direct_ptr_block);
			return numBytesWritten;
		}
		else{
			memcpy(bufferTail,direct_data_block,size-numBytesWritten);
			bio_write(super_block->d_start_blk+inode->direct_ptr[direct_ptr_index],direct_data_block);
			inode->size+=size-numBytesWritten;

			numBytesWritten+=size-numBytesWritten;
			time(& (inode->vstat.st_mtime));
			inode->vstat.st_size += numBytesWritten;
			writei(inode->ino,inode);
			free(inode);
			free(direct_data_block);
			free(indirect_data_block);
			free(direct_ptr_block);
			return numBytesWritten;
		}
		numByteOffset=0;
	}
}
	if(numBytesWritten>=size){
		time(& (inode->vstat.st_mtime));
		inode->vstat.st_size += numBytesWritten;
		writei(inode->ino,inode);
		free(inode);
		free(direct_data_block);
		free(indirect_data_block);
		free(direct_ptr_block);
		return numBytesWritten;
	}
	for(indirect_ptr_index=0;indirect_ptr_index<8;indirect_ptr_index++){
		if(inode->indirect_ptr[indirect_ptr_index]==-1){
			inode->indirect_ptr[indirect_ptr_index]=get_avail_blkno();
			initialize_direct_ptr_block(super_block->d_start_blk+inode->indirect_ptr[indirect_ptr_index]);
		}
			
			bio_read(super_block->d_start_blk+inode->indirect_ptr[indirect_ptr_index],indirect_data_block);

			for(direct_ptr_index=0;direct_ptr_index<BLOCK_SIZE/sizeof(int);direct_ptr_index++){
				previouslyUnallocated=0;
				if(indirect_data_block[direct_ptr_index]==-1){
					indirect_data_block[direct_ptr_index]=get_avail_blkno();
					bio_write(super_block->d_start_blk+inode->indirect_ptr[indirect_ptr_index],indirect_data_block);
					previouslyUnallocated = 1;
				}

				bio_read(super_block->d_start_blk+indirect_data_block[direct_ptr_index],direct_data_block);

				if(BLOCK_SIZE-numByteOffset<size-numBytesWritten){
					memcpy(direct_data_block,bufferTail,BLOCK_SIZE-numByteOffset);
					bio_write(super_block->d_start_blk+indirect_data_block[direct_ptr_index],direct_data_block);
					if(previouslyUnallocated==1){
						inode->size+=BLOCK_SIZE-numByteOffset;
					}
					bufferTail+=BLOCK_SIZE-numByteOffset;
					numBytesWritten+=BLOCK_SIZE-numByteOffset;
				}
				else{
					memcpy(direct_data_block,bufferTail,size-numBytesWritten);
					bio_write(super_block->d_start_blk+indirect_data_block[direct_ptr_index],direct_data_block);
					if(previouslyUnallocated==1){
						inode->size+=size-numBytesWritten;
					}
					numBytesWritten+=size-numBytesWritten;
					time(& (inode->vstat.st_mtime));
					inode->vstat.st_size += numBytesWritten;
					writei(inode->ino,inode);
					free(inode);
					free(direct_data_block);
					free(indirect_data_block);
					free(direct_ptr_block);
					return numBytesWritten;
				}
				numByteOffset=0;

			}
		}

		if(size-numBytesWritten>0){
			time(& (inode->vstat.st_mtime));
			inode->vstat.st_size += numBytesWritten;
			writei(inode->ino,inode);
			free(inode);
			free(direct_data_block);
			free(indirect_data_block);
			free(direct_ptr_block);
			return numBytesWritten;
		}
	}else{
  int indirectOffset = offset - 16*BLOCK_SIZE; //
  int numOffsetBlocks = indirectOffset/BLOCK_SIZE;
  int numDirectBlocksPerIndirectPtr = BLOCK_SIZE/sizeof(int);
  int numIndirectOffsetBlocks = numOffsetBlocks/numDirectBlocksPerIndirectPtr;
  int directBlockOffset = numOffsetBlocks%numDirectBlocksPerIndirectPtr;
  numByteOffset = indirectOffset%BLOCK_SIZE;
	if(numIndirectOffsetBlocks>=8){
		free(inode);
		free(direct_data_block);
		free(indirect_data_block);
		free(direct_ptr_block);
		return -1;
	}


for(indirect_ptr_index=numIndirectOffsetBlocks;indirect_ptr_index<8;indirect_ptr_index++){
		if(inode->indirect_ptr[indirect_ptr_index]==-1){ 
			inode->indirect_ptr[indirect_ptr_index]=get_avail_blkno();
			initialize_direct_ptr_block(super_block->d_start_blk+inode->indirect_ptr[indirect_ptr_index]);
		}

    bio_read(super_block->d_start_blk+inode->indirect_ptr[indirect_ptr_index],indirect_data_block);
    for(direct_ptr_index=directBlockOffset;direct_ptr_index<numDirectBlocksPerIndirectPtr;direct_ptr_index++){

			previouslyUnallocated=0;
			if(indirect_data_block[direct_ptr_index]==-1){
				indirect_data_block[direct_ptr_index]=get_avail_blkno();
				bio_write(super_block->d_start_blk+inode->indirect_ptr[indirect_ptr_index],indirect_data_block);
				previouslyUnallocated = 1;
			}

      bio_read(super_block->d_start_blk+indirect_data_block[direct_ptr_index],direct_data_block);

      if(BLOCK_SIZE-numByteOffset<size-numBytesWritten){
        memcpy(direct_data_block,bufferTail,BLOCK_SIZE-numByteOffset);
        bio_write(super_block->d_start_blk+indirect_data_block[direct_ptr_index],direct_data_block);
				if(previouslyUnallocated==1){
					inode->size+=BLOCK_SIZE-numByteOffset;
				}
        bufferTail+=BLOCK_SIZE-numByteOffset;
        numBytesWritten+=BLOCK_SIZE-numByteOffset;
      }
      else{
        memcpy(direct_data_block,bufferTail,size-numBytesWritten);
        bio_write(super_block->d_start_blk+indirect_data_block[direct_ptr_index],direct_data_block);
				if(previouslyUnallocated==1){
					inode->size+=size-numBytesWritten;
				}
        numBytesWritten+=size-numBytesWritten;
        time(& (inode->vstat.st_mtime));
		inode->vstat.st_size += numBytesWritten;
        writei(inode->ino,inode);
				free(inode);
				free(direct_data_block);
				free(indirect_data_block);
				free(direct_ptr_block);
        return numBytesWritten;
      }
      numByteOffset=0;

    }
  }
}
	inode->vstat.st_size += numBytesWritten;
	free(inode);
	free(direct_data_block);
	free(indirect_data_block);
	free(direct_ptr_block);
	return numBytesWritten;
}

static int rufs_unlink(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target filename name

	// Step 2: Call get_node_by_path() to get inode of target filename

	// Step 3: Clear data block bitmap of target filename

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target filename in its parent directory

	return 0;
}

static int rufs_truncate(const char *path, off_t size) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int rufs_release(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int rufs_flush(const char * path, struct fuse_file_info * fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int rufs_utimens(const char *path, const struct timespec tv[2]) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}


static struct fuse_operations rufs_ope = {
	.init		= rufs_init,
	.destroy	= rufs_destroy,

	.getattr	= rufs_getattr,
	.readdir	= rufs_readdir,
	.opendir	= rufs_opendir,
	.releasedir	= rufs_releasedir,
	.mkdir		= rufs_mkdir,
	.rmdir		= rufs_rmdir,

	.create		= rufs_create,
	.open		= rufs_open,
	.read 		= rufs_read,
	.write		= rufs_write,
	.unlink		= rufs_unlink,

	.truncate	= rufs_truncate,
	.flush		= rufs_flush,
	.utimens	= rufs_utimens,
	.release	= rufs_release
};


int main(int argc, char *argv[]) {
	int fuse_stat;

	getcwd(diskfile_path, PATH_MAX);
	strcat(diskfile_path, "/DISKFILE");

	fuse_stat = fuse_main(argc, argv, &rufs_ope, NULL);

	return fuse_stat;
}

