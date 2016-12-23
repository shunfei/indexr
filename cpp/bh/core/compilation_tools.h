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

#ifndef EE18D2D3_6378_4934_9E0E_E235DA6664F4
#define EE18D2D3_6378_4934_9E0E_E235DA6664F4

#include "Query.h"

#define TABLE_YET_UNSEEN_INVOLVED 2

#define ASSERT_MYSQL_STRING(x) ( assert(!x.str[x.length] && "Verification that struct st_lex_string::str (== LEX_STRING::str) ends with '\0'" ) )

#define EMPTY_TABLE_CONST_INDICATOR "%%TMP_TABLE%%"


class ReturnMeToMySQLWithError {};

const char * TablePath(TABLE_LIST * tab);
Item * UnRef(Item * item);
int TableUnmysterify(TABLE_LIST * tab, const char *& database_name, const char *& table_name, const char *& table_alias, const char *& table_path);
int OperationUnmysterify(Item * item, ColOperation& oper, bool& distinct, const int group_by_clause);

//bool Item2ConstTerm(Item * an_arg, CQTerm *term,ValueSet *v=0, bool negative = false);

//AttributeType SelectType(Query &q, CompiledQuery * cq, CQTerm *term, int how_many);


void PrintItemTree(Item* item, int indent = 0);
void PrintItemTree(char const* info, Item* item);

#endif

