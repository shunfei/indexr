
#ifndef RESOURCEMANAGERBASE_H
#define RESOURCEMANAGERBASE_H

class ResourceManagerBase {
	public:
	
		virtual ~ResourceManagerBase();
	
		virtual int GetMemoryScale() = 0;
};

#endif

