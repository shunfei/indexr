#ifndef RESOURCEMANAGER_H
#define RESOURCEMANAGER_H

#include "system/ResourceManagerBase.h"
#include "system/ResourceManagerPolicy.h"

class ResourceManager : public ResourceManagerBase {
		ResourceManagerPolicy *m_memory;
		
	public:
		ResourceManager();
		~ResourceManager();
	
		int GetMemoryScale() { return m_memory->estimate(); }
};

#endif

