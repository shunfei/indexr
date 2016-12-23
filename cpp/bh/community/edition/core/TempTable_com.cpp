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

#include "../local.h"
#include "core/TempTable.h"
#include "core/Filter.h"
#include "core/Query.h"

#include "vc/SingleColumn.h"
#include "vc/ConstExpressionColumn.h"
#include "vc/ConstColumn.h"
#include "vc/SubSelectColumn.h"
#include "vc/InSetColumn.h"
#include "vc/TypeCastColumn.h"


TempTable::TempTable(JustATable* t, int alias, Query* q) :
		mem_scale(-1), output_mind(), group_by(false), is_vc_owner(true), materialized(false), manage_used_tables(false), has_temp_table(false),
	 	m_conn(ConnectionInfoOnTLS.Get())
{
	filter.table = this;
	tables.push_back(t);
	aliases.push_back(alias);

	if(t->TableType() == TEMP_TABLE) {
		has_temp_table = true;
		if(q->IsRoughQuery())
			((TempTable *)t)->RoughMaterialize(false, NULL, true);
		else
			((TempTable *)t)->Materialize(false, NULL, true);
		filter.mind->AddDimension_cross(t->NoObj());
	} else {
		filter.mind->AddDimension_cross(t->NoObj());
	}
	if(filter.mind->TooManyTuples())
		no_obj = NULL_VALUE_64;			// a big, improper number, which we hope to be changed after conditions are applied
	else
		no_obj = filter.mind->NoTuples();
	no_cols = 0;
	query = q;
	displayable_attr = NULL;
	for_rough_query = false;
	no_global_virt_cols = 0;
	lazy = false;
	no_materialized = 0;
	is_sent = false;
	rough_is_empty = BHTRIBOOL_UNKNOWN;
}

void TempTable::JoinT(JustATable* t, int alias, JoinType jt)
{
	if(jt != JO_INNER)
		throw NotImplementedRCException("left/right/outer join is not implemented.");
	tables.push_back(t);
	aliases.push_back(alias);

	if(t->TableType() == TEMP_TABLE) {
		has_temp_table = true;
		((TempTable *)t)->Materialize();
		filter.mind->AddDimension_cross(t->NoObj());
	} else
		filter.mind->AddDimension_cross(t->NoObj());

	join_types.push_back(jt);

	if( filter.mind->TooManyTuples() )
		no_obj = NULL_VALUE_64;			// a big, improper number, which we hope to be changed after conditions are applied
	else
		no_obj = filter.mind->NoTuples();
}

VirtualColumn* CreateVCCopy(VirtualColumn* vc) 
{
	if(vc->IsSingleColumn()) {
		return new SingleColumn(* static_cast<const SingleColumn*>(vc));
	} else if(dynamic_cast<ExpressionColumn*>(vc)) {
		return new ExpressionColumn(* static_cast<const ExpressionColumn*>(vc));
	} else if(dynamic_cast<ConstExpressionColumn*>(vc)) {
		return new ConstExpressionColumn(* static_cast<const ConstExpressionColumn*>(vc));
	} else if(dynamic_cast<ConstColumn*>(vc)) {
		return new ConstColumn(* static_cast<const ConstColumn*>(vc));
	} else if(vc->IsInSet()) {
		return new InSetColumn(* static_cast<const InSetColumn*>(vc));
	} else if(vc->IsSubSelect()) {
		return new SubSelectColumn(* static_cast<const SubSelectColumn*>(vc));
	} else if(dynamic_cast<TypeCastColumn*>(vc))
		return new TypeCastColumn(* static_cast<const TypeCastColumn*>(vc));
	else {
		assert(0 && "Cannot copy VC");
		return vc;
	}
}

