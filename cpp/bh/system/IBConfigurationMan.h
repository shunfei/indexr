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

#ifndef IB_CONFIGURATIONMAN_H
#define IB_CONFIGURATIONMAN_H

#include <exception>
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include "system/Configuration.h"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/lexical_cast.hpp>

using boost::property_tree::ptree;
typedef boost::error_info<struct tag_errno_code,int> errno_code;

enum ConfValueType {
	ELEMENT_TYPE_INT, 
	ELEMENT_TYPE_STRING, 
	ELEMENT_TYPE_FLOAT, 
	ELEMENT_TYPE_NODE 
};

/** \class NodeElement
 *  \brief Represents a node/leaf of a XML document. It also keeps 
 * relations (parent, child) between the nodes.
 * name  - node/leaf name.
 * value - value of a leaf. e.g. It is 2 for PrefetchDepth. There is no value for a node.
 * valuetype - data type of value.
 * attrname  - attribute of a node. Only one attribute is kept.
 * attrvalue - value of the attribute.
 *
 * <brighthouse >
 *    <version> 1.0.0 </version>
 *    <Prefetch type="static">
 *	     <Prefetchthreads > 4 </Prefetchthreads>
 *	     <PrefetchQueueLength > 32 </PrefetchQueueLength>
 *	     <PrefetchDepth > 2 </PrefetchDepth>
 *    </prefetch>
 *    <ServerCompressedHeapSize> 400 </ServerCompressedHeapSize>
 *    <HugefileDir> </HugefileDir>
 *    <ClusterSize> 2000 </ClusterSize>
 *    <CachingLevel> 1 </CachingLevel>
 *    <BufferingLevel> 2 </BufferingLevel>
 *    <! for future add new subnodes, leafs, and define that property in Configuration manager/>
 *    <....>..</>
 * </brighthouse>
 *
 * Configuration manager class declares a number of NodeElement objects based on 
 * configuration definition of brighthouse engine and creates a tree structure.
 * External users can use configuration manager to get configuration values.
 */

class NodeElement
{
public:
	// Element property
	std::string name, value;
	std::string attrname, attrvalue;
        ConfValueType valuetype;

	// Relation to parent or child in configuration tree
	//NodeElement* parent;
    std::map<std::string, NodeElement*> childs;

	// Full xpath: All nodes from root to this node are concatened using '.' . e.g. brighthouse.prefetch.PrefetchDepth
	std::string nodepath;

	int debugon;
	// Defines a node or element. A node can have leafs and/or subnodes
	NodeElement(
		std::string _name, 
		std::string defaultvalue, 
		ConfValueType _valuetype, 
		std::string _attrname);

	~NodeElement()
	{
		Delete(this);
	}

	void SetDebugOn(int _debugon)
	{
		debugon = _debugon;
	}
	
	void AddChild(NodeElement* e);

	int LoadData(NodeElement *node, ptree* pt, std::string &error);
	// Some functions for debugging
	void TraverseTree(NodeElement *node, std::string &conf, int level=1)
	{
		if (!node)
			return;
		std::map<std::string, NodeElement*>::iterator it;
		conf += "node:" + node->name + "\n";
		for ( it=node->childs.begin(); it != node->childs.end(); it++)
		{
			for (int i=0; i < level; i++)
				conf += "\t";
			conf += "subnode/leaf:" + it->first + "\n";
			if ((*it).second->childs.size() > 0)
				TraverseTree((*it).second, conf, level+1);
		}
	}
	
	void TraverseLeafs(NodeElement *node, std::string &conf, int level=1)
	{
		if (!node)
			return;
		std::map<std::string, NodeElement*>::iterator it;
		if (node->childs.size() == 0) //leaf
		{
			conf += "\nOption: " + node->nodepath + ", value: " + node->value;
			return;
		}
		for ( it=node->childs.begin(); it != node->childs.end(); it++)
			TraverseLeafs((*it).second, conf, level+1);
	}
	
	void Delete(NodeElement *node)
	{
		if (!node)
			return;
		std::map<std::string, NodeElement*>::iterator it;
		for ( it=node->childs.begin(); it != node->childs.end(); it++)
		{
			Delete((*it).second);
			delete (*it).second;
			(*it).second = NULL;
		}
		node->childs.empty();
	}
};

/** \class ConfigurationManager
 *  \brief Defines configuration, load configuration and provide values to external callers
 *  conftree - boost property tree is used to load xml file into a tree
 *  NodeElement* root - brighthouse configuration tree. Read nodes from conftree based on
 *  brighhtouse advanced configuration structure and loads into this tree.
 */
class IBConfigurationManager
{
private:
	void DeleteElements();
	std::string confpath;
public:
	int loadstatus;
	int debugon;

	ptree conftree;
	NodeElement* root;

	IBConfigurationManager();
	~IBConfigurationManager();

	std::string TraverseTree();
	std::string PrintConfigurations();
	void SetConfPath(std::string _confpath);

	int LoadConfig(std::string &error);
	int IsLoaded();
	
	void SetDebugOn(int _debugon);

	NodeElement* GetElement(std::string xpath, std::string &error);
	std::string GetAttrValue(std::string xpath, std::string &error);

	/** \brief GetValue* function returns value of a leaf node.
	 *  \param xpath = xpath of the leaf in property tree. e.g.  
	 *    brighthouse/prefetch/Prefetchthreads for leaf Prefetchthreads
	 *  \return string/int/float
	 */
	std::string GetValueString(std::string xpath, std::string &error);
	float GetValueFloat(std::string xpath,  std::string &error);
	int   GetValueInt(std::string xpath,    std::string &error);

	void PublishElementsToShowVar();
};

#endif
