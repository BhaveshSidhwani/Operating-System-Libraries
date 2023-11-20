/* Group Members: 
    Bhavesh Veersen Sidhwani (bs1061)
    Shobhit Singh (ss4363)
*/
// iLab Server: cp.cs.rutgers.edu


#include "my_vm.h"

// Flag to check if physical memory and data structures are initialized
int is_init = 0;

void* physical_mem;                 // Physical memory
unsigned long virtual_page_num;     // Number of pages available in virtual address space
unsigned long physical_page_num;    // Number of pages available in physical address space

unsigned char* physical_bitmap;     // Bitmap for physical address space
unsigned char* virtual_bitmap;      // Bitmap for virtual address space
unsigned char* tlb_bitmap;          // Bitmap for TLB

int offset_bits;                    // Number of offset bits in the virtual address
int internal_bits;                  // Number of bits for page table in virtual address
int external_bits;                  // Number of bits for page dirrectory in virtual address

page_table_entry* page_dir;         // Page Directory

double tlb_hit_count = 0;           // Number of times the address was found in the TLB
double tlb_miss_count = 0;          // Number of times the address was not found in the TLB
double tlb_lookup_count = 0;        // Number of times TLB was accessed to look up an address

pthread_mutex_t memory_lock = PTHREAD_MUTEX_INITIALIZER;    // Pthread Mutex

// Function to get index for virtual bitmap from virtual address
int get_virt_index(void* va) {
    return ((va - (void*)PGSIZE)/PGSIZE);
}

// Function to get index for physical bitmap from physical address
int get_phy_index(void* pa) {
    return ((pa - (void*)physical_mem)/PGSIZE);
}

// Function to get page directory's bits
int get_external_bits(void* va) {
    return ((unsigned long)va >> (offset_bits+internal_bits));
}

// Function to get page_table's bits
int get_internal_bits(void* va) {
    unsigned long mask = (1 << internal_bits) - 1;
    return ((unsigned long)va >> offset_bits) & mask;
}

// Function to get offset bits
int get_offset_bits(void* va) {
    unsigned long mask = (1 << offset_bits) - 1;
    return ((unsigned long)va & mask);
}

// Function to get virtual address from index of virtual bitmap
void* get_virt_address(int virt_index) {
    void* virt_addr = (void*) (PGSIZE * (virt_index+1));
    return virt_addr;
}

// Function to get physical address from index of physical bitmap
void* get_phy_address(int phy_index) {
    void* phy_addr = (void*) ((PGSIZE*phy_index) + (void*)physical_mem);
    return phy_addr;
}

// Function to get virtual address without the offset bits
void* get_base_virt_addr(void* va) {
    return (void*) ( ((unsigned long)va) >> offset_bits );
}

/*
Function responsible for allocating and setting your physical memory 
*/
void set_physical_mem() {
    //HINT: Also calculate the number of physical and virtual pages and allocate
    //virtual and physical bitmaps and initialize them

    // Critical section start
    pthread_mutex_lock(&memory_lock);

    //Allocate physical memory using mmap or malloc; this is the total size of
    //your memory you are simulating
    physical_mem = (void*) malloc(MEMSIZE);

    // Calculating the number of pages in physical and virtual address space
    physical_page_num = MEMSIZE / PGSIZE;
    virtual_page_num = MAX_MEMSIZE / PGSIZE;

    // Calculating number of external, internal and offset bits
    offset_bits = log2(PGSIZE);
    internal_bits = log2(PGSIZE/sizeof(pte_t));
    external_bits = 32 - offset_bits - internal_bits;
    
    // Allocating physcial, virtual and TLB bitmaps
    physical_bitmap = (unsigned char*) malloc(physical_page_num * (sizeof(unsigned char)));
    virtual_bitmap = (unsigned char*) malloc(virtual_page_num * (sizeof(unsigned char)));
    tlb_bitmap = (unsigned char*) malloc(TLB_ENTRIES * (sizeof(unsigned char)));
    
    // Initializing the values in bitmap to 0
    for (int i=0; i<physical_page_num; i++) {
        physical_bitmap[i] = 0;
    }
    for (int i=0; i<virtual_page_num; i++) {
        virtual_bitmap[i] = 0;
    }
    for (int i=0; i<TLB_ENTRIES; i++) {
        tlb_bitmap[i] = 0;
    }

    // Initializing the page directory
    int dir_size = 1 << external_bits;
    printf("\n");
    page_dir = (page_table_entry*) malloc(dir_size * sizeof(page_table_entry*));
    for (int i=0; i<dir_size; i++) {
        page_dir[i].valid = 0;
    }

    // Initializing TLB
    tlb_store = (struct tlb*) malloc(sizeof(struct tlb)*TLB_ENTRIES);
    for (int i=0; i<TLB_ENTRIES; i++) {
        tlb_store[i].valid = 0;
    }

    // Critical Section End
    pthread_mutex_unlock(&memory_lock);
}


/*
 * Part 2: Add a virtual to physical page translation to the TLB.
 * Feel free to extend the function arguments or return type.
 */
int add_TLB(void *va, void *pa) {
    /*Part 2 HINT: Add a virtual to physical page translation to the TLB */

    // Incrementing the TLB miss count
    tlb_miss_count += 1;
    // Adding the entry in the TLB at tlb_index position
    // printf("\tadd_tlb...\n");
    int tlb_index = ((unsigned int)va) % TLB_ENTRIES;
    tlb_store[tlb_index].VPN = va;
    tlb_store[tlb_index].PPN = pa;
    tlb_store[tlb_index].valid = 1;
    return 0;
}


/*
 * Part 2: Check TLB for a valid translation.
 * Returns the physical page address.
 * Feel free to extend this function and change the return type.
 */
pte_t* check_TLB(void *va) {
    /* Part 2: TLB lookup code here */

    // Incrementing the TLB lookup count
    tlb_lookup_count++;
    // Checking the TLB at tlb_index position for a valid entry
    // printf("\tcheck_tlb...\n");
    int tlb_index = ((unsigned int)va) % TLB_ENTRIES;
    if (tlb_store[tlb_index].valid == 1) {
        if (tlb_store[tlb_index].VPN == va) {
            tlb_hit_count++;
            return (pte_t*) (tlb_store[tlb_index].PPN);
        }
    }

    // return NULL if not found in the TLB
    return NULL;
   /*This function should return a pte_t pointer*/
}


/*
 * Part 2: Print TLB miss rate.
 * Feel free to extend the function arguments or return type.
 */
void print_TLB_missrate() {
    double miss_rate = 0;
    miss_rate = tlb_miss_count / tlb_lookup_count;
    miss_rate *= 100;

    /*Part 2 Code here to calculate and print the TLB miss rate*/

    fprintf(stderr, "TLB miss rate %lf \n", miss_rate);
}



/*
The function takes a virtual address and page directories starting address and
performs translation to return the physical address
*/
pte_t *translate(pde_t *pgdir, void *va) {
    /* Part 1 HINT: Get the Page directory index (1st level) Then get the
    * 2nd-level-page table index using the virtual address.  Using the page
    * directory index and page table index get the physical address.
    *
    * Part 2 HINT: Check the TLB before performing the translation. If
    * translation exists, then you can return physical address from the TLB.
    */

    // Check if the virtual address provided if valid or not
    if (page_dir == NULL || va == NULL) {
        return NULL;
    } else {
        int index = get_virt_index(va);
        // printf("\tvirtual_bitmap[index] == 0...\n");
        if (virtual_bitmap[index] == 0) {
            return NULL;
        }
    }

    // Get the internal, external and offset bits from virtual address
    int page_dir_index = get_external_bits(va);
    int page_table_index = get_internal_bits(va);
    int offset = get_offset_bits(va);
    // Get the VPN from virtual address
    void* base_va = get_base_virt_addr(va);

    // Check TLB if the VPN entry exists
    void* pa = (void*) check_TLB(base_va);
    if (pa != NULL) {
        // Add the offset to the physical address
        return (pte_t*)((unsigned long)pa+offset);
    }

    // Check if the page directory entry is valid
    // printf("\tpage_dir[page_dir_index].valid == 0\n");
    if (page_dir[page_dir_index].valid == 0) {
        return NULL;
    } else {
        page_table_entry* pt = page_dir[page_dir_index].PPN;
        // Check if the page table entry is valid
        // printf("\tpt[page_table_index].valid == 0...\n");
        if (pt[page_table_index].valid == 0) {
            return NULL;
        } else {
            // If page table entry is valid then
            // add the Physical address to the TLB
            add_TLB(base_va, pt[page_table_index].PPN);
            return (pte_t*)(((unsigned long)pt[page_table_index].PPN) + offset);
        }
    }

    //If translation not successful, then return NULL
    return NULL; 
}


/*
The function takes a page directory address, virtual address, physical address
as an argument, and sets a page table entry. This function will walk the page
directory to see if there is an existing mapping for a virtual address. If the
virtual address is not present, then a new entry will be added
*/
int page_map(pde_t *pgdir, void *va, void *pa) {
    /*HINT: Similar to translate(), find the page directory (1st level)
    and page table (2nd-level) indices. If no mapping exists, set the
    virtual to physical mapping */

    // Get the internal and external bits from virtual address
    int page_dir_index = get_external_bits(va);
    int page_table_index = get_internal_bits(va);

    void* base_va = get_base_virt_addr(va);

    // If page table does not exist then create it
    if (page_dir[page_dir_index].valid == 0) {
        int pt_size = pow(2, internal_bits) * sizeof(page_table_entry);
        page_dir[page_dir_index].PPN = (page_table_entry*) malloc(pt_size);
        page_dir[page_dir_index].valid = 1;
    }

    // Add the page table entry
    page_table_entry* pt = page_dir[page_dir_index].PPN;
    pt[page_table_index].PPN = pa;
    pt[page_table_index].valid = 1;
    add_TLB(base_va, pa);
    tlb_lookup_count++;
    return 0;
}


/*Function that gets the next available page in virtual address space
*/
// get_next_avail() to get_next_avail_virtual()
void *get_next_avail_virtual(int num_pages) {
    //Use virtual address bitmap to find the next free page

    int base;
    int count=0;

    // Loop from start to end of the virtual bitmap to get continuous free pages
    for (int i=0; i < virtual_page_num; i++) {
        if (virtual_bitmap[i] == 0) {
            if (count == 0) {
                base = i;
            }
            count++;
        } else {
            count = 0;
            base = 0;
        }

        if (count == num_pages) {
            break;
        }
    }

    if (count != num_pages) {
        return NULL;
    }

    // Get the addresses of the free pages
    pte_t* virt_addr = (pte_t*) malloc(sizeof(pte_t*)*num_pages);
    int k = 0;
    for (int j=base; j<(base+num_pages); j++) {
        virt_addr[k] = (pte_t*) get_virt_address(j);
        k++;
        virtual_bitmap[j] = 1;
    }

    return (void*) virt_addr;
}

/*Function that gets the next available page in physical address space
*/
void *get_next_avail_physical(int num_pages) {
    int page_index[num_pages];
    int count = 0;

    // Loop from start to end of the physical bitmap to get free pages
    for (int i=0; i<physical_page_num; i++) {
        if (physical_bitmap[i] == 0) {
            page_index[count] = i;
            count++;
        }

        if (count == num_pages) {
            break;
        }
    }

    if (count != num_pages) {
        return NULL;
    }

    // Get the addresses of the free pages
    pte_t* phy_addr = (pte_t*) malloc(sizeof(pte_t*)*num_pages);
    int k=0;
    for (int j=0; j<num_pages; j++) {
        phy_addr[k] = (pte_t*) get_phy_address(page_index[j]);
        k++;
        physical_bitmap[page_index[j]] = 1;
    }
    return (void*) phy_addr;
}


/* Function responsible for allocating pages
and used by the benchmark
*/
void* t_malloc(unsigned int num_bytes) {
    /* 
     * HINT: If the physical memory is not yet initialized, then allocate and initialize.
     */

   /* 
    * HINT: If the page directory is not initialized, then initialize the
    * page directory. Next, using get_next_avail_virtual(), check if there are free pages. If
    * free pages are available, set the bitmaps and map a new page. Note, you will 
    * have to mark which physical pages are used. 
    */

    // If Physical memory is not initialized, initialize it
    if (is_init == 0) {
        is_init = 1;
        set_physical_mem();
    }

    // Check if number of bytes is 0
    if (num_bytes == 0) {
        printf("Cannot allocate 0 bytes\n");
        return NULL;
    }

    // Calculate the number of pages required for the input num_bytes
    int num_pages = ceil((double) num_bytes/PGSIZE);

    // Critical Section Start
    pthread_mutex_lock(&memory_lock);
    // Get the addresses of the available pages in virtual and physical address space
    pte_t* page_virt_addr = (pte_t*) get_next_avail_virtual(num_pages);
    pte_t* page_phy_addr = (pte_t*) get_next_avail_physical(num_pages);

    if (page_virt_addr == NULL || page_phy_addr == NULL) {
        printf("No Memory Available\n");
        return NULL;
    }

    // Map the Physical page addresses to the virtual page addresses
    for (int i=0; i<num_pages; i++) {
        page_map(NULL, (void*) page_virt_addr[i], (void*) page_phy_addr[i]);
    }
    // Critical Section End
    pthread_mutex_unlock(&memory_lock);
    // Return the base virtual address
    return (void*) page_virt_addr[0];
}

/* Responsible for releasing one or more memory pages using virtual address (va)
*/
void t_free(void *va, int size) {
    /* Part 1: Free the page table entries starting from this virtual address
     * (va). Also mark the pages free in the bitmap. Perform free only if the 
     * memory from "va" to va+size is valid.
     *
     * Part 2: Also, remove the translation from the TLB
     */

    // Check if virtual address is NULL
    if (va == NULL) return;
    
    // Get the base and end index of virtual pages
    int virt_base_index = get_virt_index(va);
    int page_num = ceil((double)size / PGSIZE);
    int virt_end_index = virt_base_index + page_num;

    // Get the internal and external bits from the virtual address
    int page_dir_index = get_external_bits(va);
    int page_table_index = get_internal_bits(va);

    // Critical Section Start
    pthread_mutex_lock(&memory_lock);
    // Loop from base index to end index to de-allocate the pages
    for (int i=virt_base_index; i<virt_end_index; i++) {
        // Check if the page is already free
        if (virtual_bitmap[i] == 0) {
            printf("Double free!!");
            pthread_mutex_unlock(&memory_lock);
            return;
        }
        // Free up the physical pages
        void* phy_page_addr = (void*) translate(NULL, get_virt_address(i));
        if (phy_page_addr != NULL) {
            int phy_index = get_phy_index(phy_page_addr);
            physical_bitmap[phy_index] = 0;
        }
        // Update the page table and bitmap
        page_table_entry* pt =  page_dir[page_dir_index].PPN;
        pt[page_table_index].valid = 0;
        virtual_bitmap[i] = 0;
    }
    // Critical Section End
    pthread_mutex_unlock(&memory_lock);
}


/* The function copies data pointed by "val" to physical
 * memory pages using virtual address (va)
 * The function returns 0 if the put is successfull and -1 otherwise.
*/
int put_value(void *va, void *val, int size) {
    /* HINT: Using the virtual address and translate(), find the physical page. Copy
     * the contents of "val" to a physical page. NOTE: The "size" value can be larger 
     * than one page. Therefore, you may have to find multiple pages using translate()
     * function.
     */
    
    // If size of value is less than one page size 
    // then copy the value to the physical address
    if (size <= PGSIZE) {
        // Critical Section Start
        pthread_mutex_lock(&memory_lock);
        void* phy_addr = (void*) translate(NULL, va);
        if (phy_addr == NULL) return -1;
        memcpy(phy_addr, val, size);
        // Critical Section End
        pthread_mutex_unlock(&memory_lock);
    } else {
        // Else copy the value in multiple physical pages
        int rem_size = size;
        int copy_bytes = 0;
        void* temp_val = val;
        void* copy_loc = va;
        // Critical Section Start
        pthread_mutex_lock(&memory_lock);
        while (rem_size != 0) {
            if (rem_size > PGSIZE) {
                copy_bytes = PGSIZE;
            } else {
                copy_bytes = rem_size;
            }
            rem_size -= copy_bytes;

            void* phy_addr = (void*) translate(NULL, copy_loc);
            if (phy_addr == NULL) return -1;
            memcpy(phy_addr, temp_val, copy_bytes);

            temp_val += copy_bytes;
            copy_loc = get_virt_address(get_virt_index(copy_loc) + 1);
        }
        // Critical Section End
        pthread_mutex_unlock(&memory_lock);
    }
    /*return -1 if put_value failed and 0 if put is successfull*/
    return 0;
}


/*Given a virtual address, this function copies the contents of the page to val*/
void get_value(void *va, void *val, int size) {
    /* HINT: put the values pointed to by "va" inside the physical memory at given
    * "val" address. Assume you can access "val" directly by derefencing them.
    */

   // If size of value is less than one page size
   // then copy the contents of the physical address to the val address
    if (size <= PGSIZE) {
        // Critical Section Start
        pthread_mutex_lock(&memory_lock);
        void* phy_addr = (void*) translate(NULL, va);
        memcpy(val, phy_addr, size);
        // Critical Section End
        pthread_mutex_unlock(&memory_lock);
    } else {
        // Else copy from multiple physical pages
        int rem_size = size;
        int copy_bytes = 0;
        void* temp_val = val;
        void* copy_loc = va;
        // Critical Section Start
        pthread_mutex_lock(&memory_lock);
        while (rem_size != 0) {
            if (rem_size > PGSIZE) {
                copy_bytes = PGSIZE;
            } else {
                copy_bytes = rem_size;
            }
            rem_size -= copy_bytes;

            void* phy_addr = (void*) translate(NULL, copy_loc);
            memcpy(temp_val, phy_addr, copy_bytes);

            temp_val += copy_bytes;
            copy_loc = get_virt_address(get_virt_index(copy_loc) + 1);
        }
        // Critical Section End
        pthread_mutex_unlock(&memory_lock);
    }
}



/*
This function receives two matrices mat1 and mat2 as an argument with size
argument representing the number of rows and columns. After performing matrix
multiplication, copy the result to answer.
*/
void mat_mult(void *mat1, void *mat2, int size, void *answer) {

    /* Hint: You will index as [i * size + j] where  "i, j" are the indices of the
     * matrix accessed. Similar to the code in test.c, you will use get_value() to
     * load each element and perform multiplication. Take a look at test.c! In addition to 
     * getting the values from two matrices, you will perform multiplication and 
     * store the result to the "answer array"
     */
    int x, y, val_size = sizeof(int);
    int i, j, k;
    for (i = 0; i < size; i++) {
        for(j = 0; j < size; j++) {
            unsigned int a, b, c = 0;
            for (k = 0; k < size; k++) {
                int address_a = (unsigned int)mat1 + ((i * size * sizeof(int))) + (k * sizeof(int));
                int address_b = (unsigned int)mat2 + ((k * size * sizeof(int))) + (j * sizeof(int));
                get_value( (void *)address_a, &a, sizeof(int));
                get_value( (void *)address_b, &b, sizeof(int));
                // printf("Values at the index: %d, %d, %d, %d, %d\n", 
                //     a, b, size, (i * size + k), (k * size + j));
                c += (a * b);
            }
            int address_c = (unsigned int)answer + ((i * size * sizeof(int))) + (j * sizeof(int));
            // printf("This is the c: %d, address: %x!\n", c, address_c);
            put_value((void *)address_c, (void *)&c, sizeof(int));
        }
    }
}



