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

#include "NUMAHeapPolicy.h"
#include "common/bhassert.h"
#include "system/RCSystem.h"

using namespace std;

#ifdef USE_NUMA

#define NUMA_VERSION1_COMPATIBILITY
#include <numa.h>

NUMAHeap::NUMAHeap(size_t size) : HeapPolicy(size)
{
	m_avail = (numa_available() != -1);
	rccontrol << lock << "Numa availability: " << m_avail << unlock;

	if( m_avail ) {
		int max_nodes = numa_max_node();
		unsigned long node_size;
		if( max_nodes == 0 ) 
			node_size = size;
		else
			node_size = size/(max_nodes + 1);
			
		rccontrol << lock << "Numa nodes: " << max_nodes << " size (MB): " << (int)(node_size >> 20) << unlock;
		nodemask_t mask = numa_get_run_node_mask();
		nodemask_t tmp = mask;
		for(int i=0;i<NUMA_NUM_NODES;i++)
			if( nodemask_isset(&mask,i) ) {
				nodemask_zero(&tmp);
				nodemask_set(&tmp,i);	
				numa_set_membind(&tmp);
				// TBD: handle node allocation failures
				rccontrol << lock << "Allocating size (MB) " << (int)(node_size >> 20) << " on Numa node " << i << unlock;
				m_nodeHeaps.insert( std::make_pair( i, new TCMHeap(node_size) ) );
			}
		numa_set_membind(&mask);
	} else {
		m_nodeHeaps.insert( std::make_pair( 0, new TCMHeap(size) ) );
	}
}

NUMAHeap::~NUMAHeap()
{
	NodeHeapType::iterator it2;
	for( NodeHeapType::iterator iter= m_nodeHeaps.begin(); iter != m_nodeHeaps.end();) {
		delete iter->second;
		it2=iter++;
		m_nodeHeaps.erase(it2);
	}
}

MEM_HANDLE_BH NUMAHeap::alloc(size_t size)
{
	int node;
	void *result;
	if( m_avail )
		node = numa_preferred();
	else
		node = 0;
	NodeHeapType::iterator h = m_nodeHeaps.find(node);
	ASSERT(h!=m_nodeHeaps.end());
	
	result = h->second->alloc(size);
	if (result != NULL) {
		m_blockHeap.insert( std::make_pair( result, h->second ) );
		return result;
	} else {
		// TBD: allocate on a least distance basis
		h = m_nodeHeaps.begin();
		while( h != m_nodeHeaps.end() ) {
			result = h->second->alloc(size);
			if (result != NULL) {
				m_blockHeap.insert( std::make_pair( result, h->second ) );
				return result;
			}
			h++;
		}
		return NULL;
	}
}

void NUMAHeap::dealloc(MEM_HANDLE_BH mh)
{
	BlockHeapType::iterator h = m_blockHeap.find(mh);
	ASSERT(h!=m_blockHeap.end());
	
	h->second->dealloc(mh);
	m_blockHeap.erase(h);
}

MEM_HANDLE_BH NUMAHeap::rc_realloc(MEM_HANDLE_BH mh, size_t size)
{
	BlockHeapType::iterator h = m_blockHeap.find(mh);
	ASSERT(h!=m_blockHeap.end());
	
	return h->second->rc_realloc(mh, size);
}

size_t NUMAHeap::getBlockSize(MEM_HANDLE_BH mh)
{
	BlockHeapType::iterator h = m_blockHeap.find(mh);
	ASSERT(h!=m_blockHeap.end());
	
	return h->second->getBlockSize(mh);
}

#endif

