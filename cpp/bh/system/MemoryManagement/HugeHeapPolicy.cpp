/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#include "HugeHeapPolicy.h"
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <system/RCSystem.h>

#ifdef __GNUC__
#include <sys/mman.h>

using namespace std;

HugeHeap::HugeHeap(std::string hugedir, size_t size ) : TCMHeap(0)
{
	m_heap_frame = NULL;
	// convert size from MB to B and make it a multiple of 2MB
	m_size = 1024*1024*(size&~0x1);
	m_hs = HEAP_ERROR;
	m_fd = -1;
	
	if (!hugedir.empty() && size > 0) {
		char pidtext[12];
		strcpy(m_hugefilename, hugedir.c_str());
		sprintf(pidtext, "%d", getpid());
		strcat(m_hugefilename, "/bhhuge.");
		strcat(m_hugefilename, pidtext);
		m_fd = open(m_hugefilename, O_CREAT | O_RDWR, 0700);
		if (m_fd < 0) {
			m_hs = HEAP_OUT_OF_MEMORY;
			rccontrol << lock << "Memory Manager Error: Unable to create hugepage file: " << m_hugefilename << unlock;
			return;
		}
		// MAP_SHARED to have mmap fail immediately if not enough pages
		// MAP_PRIVATE does copy on write
		// MAP_POPULATE to create page table entries and avoid future surprises
#if defined(__sun__)
		m_heap_frame = (char*)mmap(NULL, m_size,PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0);
#else
		m_heap_frame = (char*)mmap(NULL, m_size,PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, m_fd, 0);
#endif		
		if (m_heap_frame == MAP_FAILED ) {
			unlink(m_hugefilename);
			m_hs = HEAP_OUT_OF_MEMORY;
			rccontrol << lock << "Memory Manager Error: hugepage file mmap error: " << GetErrorMessage(errno) << unlock;
			return;
		}
		
		rccontrol << lock << "Huge Heap size (MB) " << (int)(size) << unlock;
		//m_size = size;
		// manage the region as a normal 4k pagesize heap
		m_heap.RegisterArea(m_heap_frame,m_size>>kPageShift);
		m_size = size;
		m_hs = HEAP_SUCCESS;
	}
}

HugeHeap::~HugeHeap()
{
	if (m_heap_frame != NULL) {
		munmap(m_heap_frame,m_size);
	}
	if(m_fd > 0) {
		close(m_fd);
		unlink(m_hugefilename);
	}
}

#else
HugeHeap::HugeHeap(std::string hugedir, size_t size ) : TCMHeap(0), m_fd(0), m_heap_frame(NULL) { m_hugefilename[0] = 0; }
HugeHeap::~HugeHeap() {}
#endif

