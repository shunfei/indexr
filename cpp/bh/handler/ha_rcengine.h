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

#ifndef _HA_RCENGINE_H_
#define _HA_RCENGINE_H_

// mysql <--> bh interface functions

enum BH_BHEngineReturnValues {BH_LD_Successed = 100, BH_LD_Failed = 101, BH_LD_Continue = 102};

int BH_LoadData(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, char* errmsg, int len, int &errcode);

void BH_TerminateLoadBH(THD* thd);

void BH_UpdateAndStoreColumnComment(TABLE* table, int field_id, Field* source_field, int source_field_id, CHARSET_INFO *cs);

bool BH_SetStatementAllowed(THD *thd, LEX *lex);

int BH_HandleSelect(THD *thd, LEX *lex, select_result *&result_output, ulong setup_tables_done_option,
  int& res, int& optimize_after_bh, int& bh_free_join, int with_insert=false);

// Query throttling
//========================

// Depricated, will be removed later
bool BH_ThrottleQuery();
// New function signature
bool BH_ThrottleQuery(THD* thd, const char *sql, uint length);
void BH_UnThrottleQuery();
//========================

#endif

