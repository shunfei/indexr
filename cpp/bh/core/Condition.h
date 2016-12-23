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

#ifndef _CORE_CONDITION_H_
#define _CORE_CONDITION_H_

#include "CQTerm.h"
#include "Descriptor.h"

#include <set>

class Condition
{
protected:
	std::vector<Descriptor> descriptors;
public:
	virtual ~Condition() {}
	virtual void		AddDescriptor(CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_esc);
	virtual void		AddDescriptor(DescTree* tree, TempTable* t, int no_dims);
	virtual void		AddDescriptor(const Descriptor& desc);
	uint					Size() const { return (uint)descriptors.size(); }
	void				Clear() { descriptors.clear(); }
	void				EraseFirst() { descriptors.erase(descriptors.begin()); }
	Descriptor&			operator[](int i) { return descriptors[i]; }
	const Descriptor&	operator[](int i) const { return descriptors[i]; }
	virtual bool		IsType_Tree() { return false; }
	virtual void		Simplify();
	void MakeSingleColsPrivate(std::vector<VirtualColumn*>&);
};

class SingleTreeCondition : public Condition {
	DescTree*				tree;
public:
	SingleTreeCondition() { tree = NULL; }
	SingleTreeCondition(CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_esc);
	virtual ~SingleTreeCondition();
	virtual void			AddDescriptor(LogicalOperator lop, CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_esc);
	void					AddTree(LogicalOperator lop, DescTree* tree, int no_dims);
	DescTree*				GetTree() { return tree; }
	virtual bool			IsType_Tree() { return true; }
};

#endif
