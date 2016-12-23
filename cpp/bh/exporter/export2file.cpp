/* Copyright (C) 2000 MySQL AB, portions Copyright (C) 2005-2008 Infobright Inc.

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

#include "common/bhassert.h"
#include "export2file.h"
#include "core/RCEngine.h"


/*select_bh_export::select_bh_export(sql_exchange *ex)
#ifndef PURE_LIBRARY
	:	select_export(ex)
#endif
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#endif
	prepared = false;
}*/

#ifdef PURE_LIBRARY
select_bh_export::select_bh_export(select_export* se)
	:	select_export(), se(NULL), prepared(false)
{
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
}
#else
select_bh_export::select_bh_export(select_export* se)
	:	select_export(se->get_sql_exchange()), se(se), prepared(false)
{
}
#endif

int select_bh_export::prepare(List<Item> &list, SELECT_LEX_UNIT *u)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else

	bool blob_flag=0;
	unit= u;
	{
		List_iterator_fast<Item> li(list);
		Item *item;
		while ((item=li++))
		{
			if (item->max_length >= MAX_BLOB_WIDTH)
			{
				blob_flag=1;
				break;
			}
		}
	}
	field_term_length = exchange->field_term->length();
	if (!exchange->line_term->length())
		exchange->line_term=exchange->field_term;	// Use this if it exists
	field_sep_char= (exchange->enclosed->length() ? (*exchange->enclosed)[0] :
		field_term_length ? (*exchange->field_term)[0] : INT_MAX);
	escape_char=	(exchange->escaped->length() ? (*exchange->escaped)[0] : -1);
	line_sep_char= (exchange->line_term->length() ?
		(*exchange->line_term)[0] : INT_MAX);
	if (!field_term_length)
		exchange->opt_enclosed=0;
	if (!exchange->enclosed->length())
		exchange->opt_enclosed=1;			// A little quicker loop
	fixed_row_size= (!field_term_length && !exchange->enclosed->length() &&
		!blob_flag);

	prepared = true;
	return 0;
#endif
}

void select_bh_export::SetRowCount(ha_rows x)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else

	row_count = x;
#endif
}

void select_bh_export::SendOk(THD* thd)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	// GA
	::my_ok(thd, row_count);
#endif
}

sql_exchange* select_bh_export::SqlExchange()
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	return exchange;
#endif
}

mysql_bool select_bh_export::send_data(List<Item> &items)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	return se->send_data(items);
#endif
}


