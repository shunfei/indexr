
#include "edition/system/ResourceManager.h"
#include "system/Configuration.h"
#include "system/MagMemoryPolicy.h"

ResourceManager::ResourceManager()
{
	// Instantiate policies based on ConfigurationManager settings
	m_memory = new MagMemoryPolicy(Configuration::GetProperty(Configuration::ServerMainHeapSize));
}

ResourceManager::~ResourceManager()
{
	delete m_memory;
}

