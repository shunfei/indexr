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

#include "core/TempTable.h"
#include "ParametrizedFilter.h"
#include "edition/vc/VirtualColumn.h"
#include "RoughMultiIndex.h"

using namespace std;

RSValue FB2RSValue(uchar fb) 
{
	if(fb == Filter::FB_FULL)
		return RS_ALL;
	if(fb == Filter::FB_EMPTY)
		return RS_NONE;
	return RS_SOME;
}

ParameterizedFilter::ParameterizedFilter(CondType filter_type)
: table(NULL), rough_mind(NULL), /*tree(NULL),*/ 
filter_type(filter_type), is_mind_owner(true)
{
	mind = new MultiIndex();
}

ParameterizedFilter& ParameterizedFilter::operator=(const ParameterizedFilter& pf)
{
	if(this != &pf) {
		if(is_mind_owner && mind)
			delete mind;
		is_mind_owner = true;
		if(pf.mind)
			mind = new MultiIndex(*pf.mind);
		else
			mind = NULL;				// possible e.g. for a temporary data sources
		AssignInternal(pf);
	}
	return *this;
}

ParameterizedFilter::ParameterizedFilter(const ParameterizedFilter& pf, bool for_rough_query)
{
	is_mind_owner = for_rough_query ? false : true;
	if(for_rough_query)
		mind = pf.mind;
	else if(pf.mind)
		mind = new MultiIndex(*pf.mind);
	else
		mind = NULL;				// possible e.g. for a temporary data sources
	rough_mind = NULL;
	AssignInternal(pf);
}

ParameterizedFilter::~ParameterizedFilter(void)
{
	if(is_mind_owner)
		delete mind;
	delete rough_mind;
}

void ParameterizedFilter::AssignInternal(const ParameterizedFilter& pf)
{
	if(rough_mind)
		delete rough_mind;
	if(pf.rough_mind)
		rough_mind = new RoughMultiIndex(*pf.rough_mind);
	else
		rough_mind = NULL;
	for(uint i = 0; i < pf.descriptors.Size(); i++)
		if(!pf.descriptors[i].done)
			descriptors.AddDescriptor(pf.descriptors[i]);
	parametrized_desc = pf.parametrized_desc;
	table = pf.table;
	filter_type = pf.filter_type;
}

//ParameterizedFilter &ParameterizedFilter::operator=(const ParameterizedFilter& pf)
//{
//	if(this != &pf) {
//		aliases = pf.aliases;
//		join_types = pf.join_types;
//		log_connectors = pf.log_connectors;
//		delete mind;
//		mind = new MultiIndex(*pf.mind);
//		rough_mind = NULL;
//		descriptors = pf.descriptors;
//		//for(int i = 0; i < descriptors.size(); i++)
//		//	descriptors[i].SetMultiIndex(m_index);
//		parametrized_desc = pf.parametrized_desc;
//		//for(int i = 0; i < parametrized_desc.size(); i++)
//		//	parametrized_desc[i].SetMultiIndex(m_index);
//		table = pf.table;
//		table_alias = pf.table_alias;
//		filter_type = pf.filter_type;
//		reset_vc_stats = pf.reset_vc_stats;
//		is_mind_owner = pf.is_mind_owner;
//	}
//	return *this;
//}

void ParameterizedFilter::AddConditions(const Condition* new_cond)
{
	const Condition& cond = *new_cond;
	for(uint i = 0; i < cond.Size(); i++)
		if(cond[i].IsParameterized())
			parametrized_desc.AddDescriptor(cond[i]);
		else
			descriptors.AddDescriptor(cond[i]);
}

void ParameterizedFilter::ProcessParameters()
{
	MEASURE_FET("ParameterizedFilter::ProcessParameters(...)");
	descriptors.Clear();
	for(uint i = 0; i < parametrized_desc.Size(); i++)
		descriptors.AddDescriptor(parametrized_desc[i]);
	parametrized_desc.Clear();
}

void ParameterizedFilter::PrepareRoughMultiIndex()
{
	MEASURE_FET("ParameterizedFilter::PrepareRoughMultiIndex(...)");
	if(!rough_mind && mind) {
		vector<int> packs;
		for(uint i = 0; i < (uint)mind->NoDimensions(); i++)
			packs.push_back(bh::common::NoObj2NoPacks(mind->OrigSize(i)));
		rough_mind = new RoughMultiIndex(packs);
		for(uint d = 0; d < (uint)mind->NoDimensions(); d++) {
			Filter* f = mind->GetFilter(d);
			for(int p = 0; p < rough_mind->NoPacks(d); p++) {
				if(f == NULL)
					rough_mind->SetPackStatus(d, p, RS_UNKNOWN);
				else if(f->IsFull(p))
					rough_mind->SetPackStatus(d, p, RS_ALL);
				else if(f->IsEmpty(p))
					rough_mind->SetPackStatus(d, p, RS_NONE);
				else
					rough_mind->SetPackStatus(d, p, RS_SOME);
			}
		}
	}
}
