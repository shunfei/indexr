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

#include "common/CommonDefinitions.h"
#include "edition/local.h"
#include "core/Query.h"
#include "vc/ConstColumn.h"
#include "vc/ConstExpressionColumn.h"
#include "vc/TypeCastColumn.h"
#include "vc/SingleColumn.h"
#include "vc/InSetColumn.h"
#include "core/MysqlExpression.h"
#include "core/compilation_tools.h"

using namespace std;

VirtualColumn* Query::BuildIBExpression(Item* item, ColumnType result_type, TempTable* temp_table, int temp_table_alias, 
										MultiIndex* mind, bool skip_fix2fix_conv, bool toplevel)
{
	return NULL;
}

VirtualColumn* Query::AddTypeCast(const ColumnType& item_type, const ColumnType& result_type, VirtualColumn* col, 
								  TempTable* temp_table, bool skip_fix2fix_conv, bool toplevel)
{
	return col;
}

VirtualColumn* Query::CreateIBExpFunction(Item_func* item_func, MultiIndex* mind, vector<VirtualColumn*> child_cols, TempTable *t)
{
	return NULL;
}

ColumnType Query::GetArgumentType(Item_func* item_func, const ColumnType& result_type, uint arg_pos)
{
	return ColumnType(RC_BIGINT);
}


bool Query::IsSupportedFunction(Item_func* ifunc)
{
	return false;
}


bool Query::IBExpressionCheck(Item* item)
{
	return false;
}

void Query::InitUserParams(ConnectionInfo *conn_info)
{}
