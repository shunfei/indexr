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

#include "IBConfigurationMan.h"
#include "system/RCSystem.h"
#include "util/BHString.h"

#define ADVANCED_CONFFILE_PREFIX ".infobright"

// Variables to show config through mysql 'show variables' command

#ifndef __BH_COMMUNITY__
int bh_sysvar_prefetch_threads;
int bh_sysvar_prefetch_queueLength;
int bh_sysvar_prefetch_depth;
int bh_sysvar_parscan_maxthreads;
int bh_sysvar_parscan_nodps_atonce;
int bh_sysvar_parscan_mindps_per_thread;
#endif
char* bh_sysvar_hugefiledir;
int bh_sysvar_clustersize;
int bh_sysvar_loadersavethreadnumber;
int bh_sysvar_cachinglevel;
int bh_sysvar_bufferinglevel;
char* bh_sysvar_mm_policy;
int bh_sysvar_mm_hardlimit;
char* bh_sysvar_mm_releasepolicy;
int bh_sysvar_mm_largetempratio;
#ifndef __BH_COMMUNITY__
int bh_sysvar_throttlelimit;
#endif
int bh_sysvar_sync_buffers;

NodeElement::NodeElement(
	std::string _name,
	std::string defaultvalue,
	ConfValueType _valuetype,
	std::string _attrname)
	: name(lowercase(_name)),
	  value(lowercase(defaultvalue)),
	  attrname(lowercase(_attrname)),
	  valuetype(_valuetype), 
	  //parent(NULL),
	  debugon(0)
{
	if (valuetype != ELEMENT_TYPE_NODE) // Leaf
		childs.empty();
	nodepath = name;
}

void NodeElement::AddChild(NodeElement* e)
{
	e->nodepath = nodepath + "." + e->name;
	childs[e->name] = e;
	e->debugon = debugon;
}

int NodeElement::LoadData(NodeElement *node, ptree* pt, std::string &error)
{
	if (debugon) std::cout << "nodepath=" << node->nodepath << "\n";
	std::map<std::string, NodeElement*>::iterator it;

	BOOST_FOREACH(ptree::value_type &v, pt->get_child(node->nodepath)) {
			if (debugon) std::cout << "loading leaf/node="<< v.first <<"\n";
			it = node->childs.find(v.first);
			if (it != node->childs.end()) {
				if (it->second->childs.size() == 0) { // leaf node
					std::string val = v.second.get_value<std::string>();
					boost::trim(val);
					std::stringstream ssvalid;
					// some type validation
					if (it->second->valuetype == ELEMENT_TYPE_INT) {
						try {
							int ival = boost::lexical_cast<int>(val.c_str());
							if (debugon) std::cout << "ival="<< ival << "\n";
							ssvalid << ival;
						} catch(boost::bad_lexical_cast const &) {
							if (debugon) std::cout << "Bad value in input." << "\n";
							ssvalid << it->second->value; // default value will be used for invalid input
						}
					} else if (it->second->valuetype == ELEMENT_TYPE_FLOAT) {
						try {
							float fval = boost::lexical_cast<float>(val.c_str());
							if (debugon) std::cout << "fval="<< fval << "\n";
							ssvalid << fval;
						} catch(boost::bad_lexical_cast const &) {
							if (debugon) std::cout << "Bad value in input." << "\n";
							ssvalid << it->second->value; // default value will be used for invalid input
						}
					} else {
						ssvalid << val;
					}
					it->second->value = ssvalid.str();
					if (debugon) std::cout << "It's a leaf, name= "<< it->first << ", value=" << ssvalid.str() << "\n";
				}
				else {                                // non leaf node
					if (debugon) std::cout << "It's a node, name= "<< it->first <<"\n";
					LoadData((*it).second, pt, error);
				}
		}
	}
	return 0;
}

IBConfigurationManager::IBConfigurationManager()
{
	debugon = 0;
	loadstatus = 1;
	root = NULL;
}

IBConfigurationManager::~IBConfigurationManager()
{
	DeleteElements();
}

void IBConfigurationManager::SetDebugOn(int _debugon)
{
	debugon = _debugon;
}

int IBConfigurationManager::LoadConfig(std::string &error)
{
	if (root) {
		if (debugon) std::cout << "Reloading configuration..." << std::endl;
		DeleteElements();
	}

	// Define brighthouse configuration structure here ===>
	// You can build a xml type structure using a parent child relationship.
	// ======================================================================
	// ===> default configuration

	// root
	root = new NodeElement("brighthouse", "", ELEMENT_TYPE_NODE, "");
	root->SetDebugOn(debugon);

	// root->version
	root->AddChild(new NodeElement("version", "1.0.0", ELEMENT_TYPE_STRING, ""));
	
	#ifndef __BH_COMMUNITY__
	// root->prefetch
	NodeElement* prefetch = new NodeElement("prefetch", "", ELEMENT_TYPE_NODE, "type");
	root->AddChild(prefetch);

	// root->prefetch->threads, queueLength, depth
	int nprefetchthreads = RMon.NoOfProcessors();
	if (nprefetchthreads > 8)
		nprefetchthreads = 8;

	std::stringstream sprefetchthreads;
	sprefetchthreads << nprefetchthreads;
  	prefetch->AddChild(new NodeElement("threads",     sprefetchthreads.str(),   ELEMENT_TYPE_INT, "type"));
  	prefetch->AddChild(new NodeElement("queuelength", "32",  ELEMENT_TYPE_INT, "type"));
  	prefetch->AddChild(new NodeElement("depth",       "2",   ELEMENT_TYPE_INT, "type"));

	NodeElement* parscan = new NodeElement("parallelscan", "", ELEMENT_TYPE_NODE, "type");
	root->AddChild(parscan);
	parscan->AddChild(new NodeElement("maxthreads",       "256",   ELEMENT_TYPE_INT, "type"));
#ifdef __GNUC__
	parscan->AddChild(new NodeElement("noDPsAtOnce",       "1",   ELEMENT_TYPE_INT, "type"));
#else
	parscan->AddChild(new NodeElement("noDPsAtOnce",       "5",   ELEMENT_TYPE_INT, "type"));
#endif
	parscan->AddChild(new NodeElement("minDPsPerThread",       "10",   ELEMENT_TYPE_INT, "type"));
	#endif
	NodeElement* paraggr = new NodeElement("parallelaggr", "", ELEMENT_TYPE_NODE, "type");
	root->AddChild(paraggr);
	paraggr->AddChild(new NodeElement("maxthreads",       "32",   ELEMENT_TYPE_INT, "type"));
	
	// root-> hugefiledir, clustersize ,loadersavethreadnumber, cachinglevel, bufferinglevel
	root->AddChild(new NodeElement("HugefileDir",    "",       ELEMENT_TYPE_STRING, "type"));
	root->AddChild(new NodeElement("ClusterSize",    "2000",   ELEMENT_TYPE_INT, "type"));
	root->AddChild(new NodeElement("LoaderSaveThreadNumber", "16", ELEMENT_TYPE_INT, "type"));
	root->AddChild(new NodeElement("CachingLevel",   "1", ELEMENT_TYPE_INT, "type"));
	root->AddChild(new NodeElement("BufferingLevel", "2", ELEMENT_TYPE_INT, "type"));

	// root->memorymanagement
	NodeElement* mm = new NodeElement("memorymanagement", "", ELEMENT_TYPE_NODE, "type");
	root->AddChild(mm);
	mm->AddChild(new NodeElement("policy", "", ELEMENT_TYPE_STRING, "type"));
	mm->AddChild(new NodeElement("hardlimit", "0", ELEMENT_TYPE_INT, "type"));
	mm->AddChild(new NodeElement("releasepolicy", "2Q", ELEMENT_TYPE_STRING, "type"));
	mm->AddChild(new NodeElement("largetempratio", "0.2",   ELEMENT_TYPE_FLOAT, "type"));

	#ifndef __BH_COMMUNITY__
	// root->throttle
	NodeElement* throttle = new NodeElement("throttle", "", ELEMENT_TYPE_NODE, "type");
	root->AddChild(throttle);
	// root->throttle->limit
	throttle->AddChild(new NodeElement("limit", "0",   ELEMENT_TYPE_INT, "type"));
	throttle->AddChild(new NodeElement("scheduler", "0",   ELEMENT_TYPE_INT, "type"));

	#endif

	// root->sync_buffers
	root->AddChild(new NodeElement("sync_buffers", "0", ELEMENT_TYPE_INT, "type"));

	// <=== End of default configuration
	// ======================================================================

#ifdef __BH_COMMUNITY__
	// In community edition, it does not read .infobright file
	return 0;
#endif

	bool AutoConfigure = Configuration::GetProperty(Configuration::AutoConfigure);
	if (!AutoConfigure) {

		#ifndef __BH_COMMUNITY__
		prefetch->childs["threads"]->value     = "0";
		prefetch->childs["queuelength"]->value = "0";
		prefetch->childs["depth"]->value       = "0";
		#endif

		if (confpath.empty()) {
			confpath = infobright_home_dir + ADVANCED_CONFFILE_PREFIX;
			if (!DoesFileExist(confpath)) {
				confpath = infobright_data_dir + ADVANCED_CONFFILE_PREFIX;
			}
			if (!DoesFileExist(confpath)) {
				error = "Infobright xml file " + confpath + " does not exist";
				return loadstatus;
			}
		}
	
		std::fstream stream(confpath.c_str(), std::ios::in);
		std::string strxml = "", line;
	
		if (!stream.is_open()) {
			error = "Could not open or read xml file " + confpath + "";
			return loadstatus;
		}
		while (!stream.eof()) {
			getline (stream, line);
			line = lowercase(line);
			line += "\n";
			strxml += line;
		}
		stream.close();
	
		std::stringstream ssxml(strxml);
		if (debugon) std::cout << "XML file : " << strxml;

		try {
			read_xml(ssxml, conftree);
		}
		catch (std::exception &e) {
			error = "Error: Boost xml parsing error ";
			error += e.what();
			return loadstatus;
		}

		try {
			loadstatus =root->LoadData(root, &conftree, error);
		}
		catch (std::exception &e) {
			error = e.what();
			loadstatus = 1;
			return loadstatus;
		}
	}

	// Some verification
	/////////////////////////////////////////

	// largetempratio is float and its range is [0.0 - 1.0]
	// Set default value for invalid input. 
	float ltempratio;
	std::stringstream ssfloat(mm->childs["largetempratio"]->value);
	ssfloat >> ltempratio;
	if (ltempratio > 1.0 || ltempratio < 0.0)
		mm->childs["largetempratio"]->value = "0.0";

	return loadstatus;
}

void IBConfigurationManager::PublishElementsToShowVar()
{
	std::string error;
	#ifndef __BH_COMMUNITY__
	bh_sysvar_prefetch_threads = GetValueInt("brighthouse/prefetch/threads", error);
	bh_sysvar_prefetch_queueLength = GetValueInt("brighthouse/prefetch/queueLength", error);
	bh_sysvar_prefetch_depth = GetValueInt("brighthouse/prefetch/depth", error);
	bh_sysvar_parscan_maxthreads = GetValueInt("brighthouse/parallelscan/maxthreads", error);
	bh_sysvar_parscan_nodps_atonce = GetValueInt("brighthouse/parallelscan/noDPsAtOnce", error);
	bh_sysvar_parscan_mindps_per_thread = GetValueInt("brighthouse/parallelscan/minDPsPerThread", error);

	#endif

	bh_sysvar_hugefiledir = const_cast<char *>(GetValueString("brighthouse/hugefiledir", error).c_str());
	bh_sysvar_clustersize = GetValueInt("brighthouse/clustersize", error);
	bh_sysvar_loadersavethreadnumber = GetValueInt("brighthouse/loadersavethreadnumber", error);
	bh_sysvar_cachinglevel = GetValueInt("brighthouse/cachinglevel", error);
	bh_sysvar_bufferinglevel = GetValueInt("brighthouse/bufferinglevel", error);

	bh_sysvar_mm_policy = const_cast<char *>(GetValueString("brighthouse/memorymanagement/policy", error).c_str());
	bh_sysvar_mm_hardlimit = GetValueInt("brighthouse/memorymanagement/", error);
	bh_sysvar_mm_releasepolicy = const_cast<char *>(GetValueString("brighthouse/memorymanagement/releasepolicy", error).c_str());
	bh_sysvar_mm_largetempratio = GetValueInt("brighthouse/memorymanagement/largetempratio", error);

	#ifndef __BH_COMMUNITY__
	bh_sysvar_throttlelimit = GetValueInt("brighthouse/throttle/limit", error);
	#endif
	bh_sysvar_sync_buffers = GetValueInt("brighthouse/sync_buffers", error);
}

int IBConfigurationManager::IsLoaded()
{
	return (loadstatus==0);
};

void IBConfigurationManager::SetConfPath(std::string _confpath)
{
	confpath = _confpath;
}

std::string IBConfigurationManager::TraverseTree()
{
	std::string sconf;
	root->TraverseTree(root, sconf);
	return sconf;
}

std::string IBConfigurationManager::PrintConfigurations()
{
	std::string sconf;
	root->TraverseLeafs(root, sconf);
	return sconf;
}

NodeElement* IBConfigurationManager::GetElement(std::string xpath, std::string &error)
{
	std::map<std::string, NodeElement*>::iterator it;
	std::vector<std::string> nodenames = tokenize(xpath, '/');

	int level = 0;
	for (level = 0; level<nodenames.size(); level++) {
		nodenames[level] = lowercase(nodenames[level]);
		if (debugon) std::cout <<nodenames[level] << "\n";
	}

	NodeElement* curnode = root;
	level = 1;

	while (curnode && level<nodenames.size()) {
		if (debugon) std::cout << "Current Level=" << level << "\n node=" << curnode->name <<"\n";
		it = curnode->childs.find(nodenames[level]);
		if (it != curnode->childs.end()) {
			curnode = it->second;
		}
		else {
			error = "Bad xpath given";
			return NULL;
		}
		level ++;
	}
	return curnode;
}

std::string IBConfigurationManager::GetAttrValue(std::string xpath, std::string &error)
{
	std::string strval("");
	NodeElement* element = GetElement(xpath, error);
	if (element)
		strval = element->attrvalue;
	return strval;
}

std::string IBConfigurationManager::GetValueString(std::string xpath, std::string &error)
{
	std::string strval("");
	NodeElement* element = GetElement(xpath, error);
	if (element) strval = element->value;
	return strval;
}

float IBConfigurationManager::GetValueFloat(std::string xpath,  std::string &error)
{
	float floatval;
	std::string strval = GetValueString(xpath, error);
	if (strval == "") strval= "0";
	std::stringstream ssfloat(strval);
	ssfloat >> floatval;
	return floatval;
}

int IBConfigurationManager::GetValueInt(std::string xpath, std::string &error)
{
	int intval = (int)GetValueFloat(xpath, error);
	return intval;
}

void IBConfigurationManager::DeleteElements()
{
	root->Delete(root);
	delete root;
	root = NULL;
}
