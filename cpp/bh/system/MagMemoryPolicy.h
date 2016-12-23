
#ifndef MAGMEMORYPOLICY_H
#define MAGMEMORYPOLICY_H

#include "system/ResourceManagerPolicy.h"

// Estimate memory policy from magnitude of configured main heap size

class MagMemoryPolicy : public ResourceManagerPolicy {
	int m_scale;
	
	public:
	
	MagMemoryPolicy(int size) {
		if(size<200)	    m_scale = 0;		// very small
		else if(size<500)	m_scale = 1;		// up to 0.5 GB
		else if(size<1200)	m_scale = 2;		// up to 1.2 GB
		else if(size<2500)	m_scale = 3;		// up to 2.5 GB
		else if(size<5000)	m_scale = 4;		// up to 5 GB
		else if(size<10000)	m_scale = 5;		// up to 10 GB
		else
			m_scale = size / 16000 + 5;  //linear scale
	}
	
	
	int estimate() { return m_scale;}
};

#endif
