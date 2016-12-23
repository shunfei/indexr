
#ifndef RESOURCEMANAGERPOLICY_H
#define RESOURCEMANAGERPOLICY_H

class ResourceManagerPolicy {
	
	public:
	
	virtual ~ResourceManagerPolicy() {}
	
	virtual int estimate() = 0;
};

#endif
