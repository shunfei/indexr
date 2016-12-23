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

#include "types/RCItemTypes.h"
#include "types/ValueParserForText.h"
#include "core/RCEngine.h"
#include "common/DataFormat.h"

using namespace std;

namespace
{
void scan_fields(List<Item> &fields, uint*& buf_lens, map<int,Item*> &items_backup);
void restore_fields(List<Item> &fields, map<int,Item*> &items_backup);
void SetTableNullRow(TABLE* table);
void UnsetTableNullRow(TABLE* table);
bool IsFieldNullable(Field* field);
void SetFieldState(Field* field, bool is_null);
}

namespace {
void scan_fields(List<Item>& fields, uint*& buf_lens, map<int,Item*>& items_backup)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	Item* item;
	Field* f;
	Item_field* ifield;
	Item_ref* iref;
	Item_sum* is;

	Item::Type item_type;
	Item_sum::Sumfunctype sum_type;
	List_iterator<Item> li(fields);

	buf_lens = new uint[fields.elements];
	uint item_id = 0;
	uint field_length = 0;
	uint total_length = 0;

	// we don't need to check each type f->pack>length() returns correct value

	// check fileds
	stdext::hash_set<size_t> used_fields;
	stdext::hash_set<size_t>::iterator iter_uf;

	Item_sum_int_rcbase* isum_int_rcbase;
	Item_sum_sum_rcbase* isum_sum_rcbase;
	Item_sum_hybrid_rcbase* isum_hybrid_rcbase;

	while((item = li++)) {
		item_type = item->type();
		buf_lens[item_id] = 0;
		switch(item_type) {
			case Item::FIELD_ITEM: {  // regular select
				ifield = (Item_field*)item;
				f = ifield->result_field;
				iter_uf = used_fields.find((size_t)f);
				if(iter_uf == used_fields.end()) {
					used_fields.insert((size_t)f);
					field_length = f->pack_length();
					buf_lens[item_id] = field_length;
					total_length += field_length;
				} else
					buf_lens[item_id] = 0;
				break;
			}
			case Item::COND_ITEM:
			case Item::SUBSELECT_ITEM:
			case Item::FUNC_ITEM: 
			case Item::REF_ITEM: {	// select from view
				Item_sum_hybrid_rcbase* tmp = new Item_sum_hybrid_rcbase();
				items_backup[item_id] = item;
				tmp->decimals = item->decimals;
				tmp->hybrid_type = item->result_type();
				tmp->unsigned_flag = item->unsigned_flag;
				tmp->hybrid_field_type = item->field_type();
				tmp->collation.set(item->collation);
				tmp->value.set_charset(item->collation.collation);
				li.replace(tmp);
				break;
			}
			case Item::SUM_FUNC_ITEM: {
				is = (Item_sum*)item;
				sum_type = is->sum_func();

				if(sum_type == Item_sum::MIN_FUNC || sum_type == Item_sum::MAX_FUNC) {
					isum_hybrid_rcbase = new Item_sum_hybrid_rcbase();
					items_backup[item_id] = item;

					// We have to add some knowledge to our item from original item
					isum_hybrid_rcbase->decimals = is->decimals;
					isum_hybrid_rcbase->hybrid_type = is->result_type();
					isum_hybrid_rcbase->unsigned_flag = is->unsigned_flag;
					isum_hybrid_rcbase->hybrid_field_type = is->field_type();
					isum_hybrid_rcbase->collation.set(is->collation);
					isum_hybrid_rcbase->value.set_charset(is->collation.collation);
					li.replace(isum_hybrid_rcbase);
					buf_lens[item_id] = 0;
					break;
				} else if(sum_type == Item_sum::COUNT_FUNC || sum_type == Item_sum::COUNT_DISTINCT_FUNC
						|| sum_type == Item_sum::SUM_BIT_FUNC) {
					isum_int_rcbase = new Item_sum_int_rcbase();
					isum_int_rcbase->unsigned_flag = is->unsigned_flag;
					items_backup[item_id] = item;
					li.replace(isum_int_rcbase);
					buf_lens[item_id] = 0;
					break;
				// we can use the same type for SUM,AVG and SUM DIST, AVG DIST
				} else if(sum_type == Item_sum::SUM_FUNC || sum_type == Item_sum::AVG_FUNC
						|| sum_type == Item_sum::SUM_DISTINCT_FUNC || sum_type == Item_sum::AVG_DISTINCT_FUNC
						|| sum_type == Item_sum::VARIANCE_FUNC || sum_type == Item_sum::STD_FUNC) {
					isum_sum_rcbase = new Item_sum_sum_rcbase();
					items_backup[item_id]=item;

					// We have to add some knowledge to our item from original item
					isum_sum_rcbase->decimals = is->decimals;
					isum_sum_rcbase->hybrid_type = is->result_type();
					isum_sum_rcbase->unsigned_flag = is->unsigned_flag;
					li.replace(isum_sum_rcbase);
					buf_lens[item_id] = 0;
					break;
				} else {
					buf_lens[item_id] = 0;
					break;
				}
			} // end case
		} // end switch
		item_id++;
	} // end while

	item_id = 0;
	li.rewind();

	while((item = li++)) {
		item_type = item->type();
		switch(item_type) {
			case Item::FIELD_ITEM:  // regular select
				ifield = (Item_field*)item;
				f = ifield->result_field;
				break;
			case Item::REF_ITEM:	// select from view
				iref = (Item_ref*)item;
				ifield = (Item_field*)(*iref->ref);
				f = ifield->result_field;
				break;
			default:
				break;
		}
		item_id++;
	}
#endif
}

void restore_fields(List<Item> &fields, map<int,Item*> &items_backup)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	//nothing to restore
	if(items_backup.size() == 0) {
		return;
	}

	Item* item;
	List_iterator<Item> li(fields);

	int count = 0;
	while((item=li++))	{
		if(items_backup.find(count) != items_backup.end())
			li.replace(items_backup[count]);
		count++;
	}
#endif
}

inline void SetTableNullRow(TABLE* table)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(table);
	table->null_row = 1;
#endif
}

inline void UnsetTableNullRow(TABLE* table)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(table);
	table->null_row = 0;
#endif
}

inline bool IsFieldNullable(Field* field)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(field);
	return (field->null_ptr != 0);
#endif
}

inline void SetFieldState(Field* field, bool is_null)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(field);
	if(is_null) {
		if(IsFieldNullable(field))
			field->set_null();
		else		// not nullable, but null needed because of outer join
			SetTableNullRow(field->table);
	} else {
		if(IsFieldNullable(field))
			field->set_notnull();
		else		// not nullable, but null needed because of outer join
			UnsetTableNullRow(field->table);
	}
#endif
}
}

ResultSender::ResultSender(THD* thd, select_result* res, List<Item>& fields) 
: thd(thd), res(res), buf_lens(NULL), fields(fields), is_initialized(false), offset(NULL), limit(NULL), rows_sent(0)
{
}

void ResultSender::Init(TempTable* t)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	thd->proc_info="Sending data";
	DBUG_PRINT("info", ("%s", thd->proc_info));
	res->send_fields(fields, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF);
	thd->lex->unit.offset_limit_cnt = 0;
	scan_fields(fields, buf_lens, items_backup);
#endif
}

void ResultSender::Send(TempTable::RecordIterator& iter)
{

	if((iter.currentRowNumber() & 0x7fff) == 0)
		if(ConnectionInfoOnTLS.Get().killed())
			throw KilledRCException();

	TempTable* owner(iter.Owner());
	if(!is_initialized) {
		owner->CreateDisplayableAttrP();
		Init(owner);
		is_initialized = true;
	}
	if(owner && !owner->IsSent())
		owner->SetIsSent();

	if(offset && *offset > 0) {
		--(*offset);
		return;
	} else if(limit) {
		if(*limit == 0)
			return;
		else
			--(*limit);
	}
	SendRecord(iter);
	rows_sent++;
}

void ResultSender::SendRecord(TempTable::RecordIterator& iter)		// send to MySQL
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	Item* item;
	Field* f;
	Item_field* ifield;
	Item_ref* iref;
	Item_sum* is;
	Item_sum_int_rcbase* isum_int_rcbase;
	Item_sum_sum_rcbase* isum_sum_rcbase;
	Item_sum_hybrid_rcbase*	isum_hybrid_rcbase;
	Item_sum::Sumfunctype sum_type;

	uint col_id = 0;
	int is_null;
	_int64 value;

	List_iterator_fast<Item> li(fields);
	li.rewind();
	while((item = li++)) {
		is_null = 0;
		RCDataType& rcdt((*iter)[col_id]);
		switch(item->type()) {
			case Item::DEFAULT_VALUE_ITEM:
			case Item::FIELD_ITEM:  // regular select
				ifield = (Item_field*)item;
				f = ifield->result_field;
				// if buf_lens[col_id] is 0 means that f->ptr was not assigned
				// because it was assigned for this instance of object
				if(buf_lens[col_id] != 0) {
					RCEngine::Convert(is_null, f, f->ptr, rcdt, NULL);
					SetFieldState(f, is_null);
				}
				break;
			case Item::REF_ITEM:	// select from view
				iref = (Item_ref*)item;
				ifield = (Item_field*)(*iref->ref);
				f = ifield->result_field;
				if(buf_lens[col_id] != 0) {
					RCEngine::Convert(is_null, f, f->ptr, rcdt, NULL);
					SetFieldState(f, is_null);
				}
				break;
			case Item::SUM_FUNC_ITEM:
				is = (Item_sum*)item;
				sum_type = is->sum_func();

				//used only MIN_FUNC
				if(sum_type == Item_sum::MIN_FUNC || sum_type == Item_sum::MAX_FUNC) {
					isum_hybrid_rcbase = (Item_sum_hybrid_rcbase*)is;
					enum_field_types is_field_type = isum_hybrid_rcbase->hybrid_field_type;
					if(isum_hybrid_rcbase->result_type() == DECIMAL_RESULT)	{
						RCEngine::Convert(is_null, isum_hybrid_rcbase->dec_value(), rcdt, item->decimals);
						//RCEngine::Convert(is_null, isum_hybrid_rcbase->dec_value(), &obj_item, item->decimals);
						isum_hybrid_rcbase->null_value = is_null;
					} else if(isum_hybrid_rcbase->result_type() == INT_RESULT) {
						RCEngine::Convert(is_null, isum_hybrid_rcbase->int64_value(), rcdt, is_field_type);
						isum_hybrid_rcbase->null_value = is_null;
					} else if(isum_hybrid_rcbase->result_type() == REAL_RESULT)	{
						RCEngine::Convert(is_null, isum_hybrid_rcbase->real_value(), rcdt);
						isum_hybrid_rcbase->null_value = is_null;
					} else if(isum_hybrid_rcbase->result_type() == STRING_RESULT) {
						RCEngine::Convert(is_null, isum_hybrid_rcbase->string_value(), rcdt, isum_hybrid_rcbase->hybrid_field_type);
						isum_hybrid_rcbase->null_value = is_null;
					}
					break;
				}
				//do not check COUNT_DISTINCT_FUNC, we use only this for both types
				if(sum_type == Item_sum::COUNT_FUNC || sum_type == Item_sum::SUM_BIT_FUNC) {
					isum_int_rcbase = (Item_sum_int_rcbase*)is;
					RCEngine::Convert(is_null, value, rcdt, is->field_type());
					if(is_null)
						value = 0;
					isum_int_rcbase->int64_value(value);
					break;
				}
				if(sum_type == Item_sum::SUM_FUNC || sum_type == Item_sum::STD_FUNC || sum_type == Item_sum::VARIANCE_FUNC) {
					isum_sum_rcbase = (Item_sum_sum_rcbase*)is;
					if(isum_sum_rcbase->result_type() == DECIMAL_RESULT) {
						RCEngine::Convert(is_null, isum_sum_rcbase->dec_value(), rcdt);
						isum_sum_rcbase->null_value = is_null;
					} else if(isum_sum_rcbase->result_type() == REAL_RESULT) {
						RCEngine::Convert(is_null, isum_sum_rcbase->real_value(), rcdt);
						isum_sum_rcbase->null_value = is_null;
					}
					break;
				}
				break;
			default:
				// Const items like item_int etc. do not need any conversion
				break;
		} // end switch
		col_id++;
	} // end while
	res->send_data(fields);
#endif
}

void ResultSender::Send(TempTable* t)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(t->IsMaterialized());
	t->CreateDisplayableAttrP();
	TempTable::RecordIterator iter = t->begin();
	TempTable::RecordIterator iter_end = t->end();
	for(; iter != iter_end; ++iter) {
		Send(iter);
	}
}

void ResultSender::Finalize(TempTable* result_table)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	if(!is_initialized) {
		is_initialized = true;
		Init(result_table);
	}
	if(result_table && !result_table->IsSent())
		Send(result_table);
	CleanUp();
	SendEof();
	rccontrol.lock(thd->thread_id) << "Result: " << rows_sent << " rows." << unlock;
#endif
}

void ResultSender::CleanUp()
{
	restore_fields(fields, items_backup);
}

void ResultSender::SendEof()
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	res->send_eof();
#endif
}

ResultSender::~ResultSender()
{
	delete [] buf_lens;
}

///////////////////////////////////////////////////////////////////////////////////////

ResultExportSender::ResultExportSender(THD* thd, select_result* result, List<Item>& fields)
	:	ResultSender(thd, result, fields)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	export_res = dynamic_cast<select_bh_export*>(result); assert(export_res);
#endif
}

void ResultExportSender::SendEof()
{
	rcbuffer->FlushAndClose();
	export_res->SetRowCount((ha_rows)rows_sent);
	export_res->SendOk(thd);
}

void init_field_scan_helpers( THD*& thd, TABLE& tmp_table, TABLE_SHARE& share )
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
  tmp_table.alias= 0;
  tmp_table.timestamp_field= 0;
  tmp_table.s= &share;
  init_tmp_table_share(thd, &share, "", 0, "", "");

  tmp_table.s->db_create_options=0;
  tmp_table.s->blob_ptr_size= portable_sizeof_char_ptr;
  tmp_table.s->db_low_byte_first = false; /* or true */
  tmp_table.null_row=tmp_table.maybe_null=0;
#endif
}

fields_t::value_type guest_field_type( THD*& thd, TABLE& tmp_table, Item*& item )
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return fields_t::value_type();
#else
	Field* field = NULL;
	Field* tmp_field = NULL;
	Field* def_field = NULL;
	if(item->type() == Item::FUNC_ITEM) {
		if(item->result_type() != STRING_RESULT)
			field = item->tmp_table_field(&tmp_table);
		else
			field = item->tmp_table_field_from_field_type(&tmp_table, 0);
	} else
		field = create_tmp_field(thd, &tmp_table, item, item->type(), (Item ***) 0, &tmp_field, &def_field, 0, 0, 0, 0, 0);
	fields_t::value_type f(MYSQL_TYPE_STRING);
	if(field) {
		f = field->type();
		delete field;
	} else
		throw DatabaseRCException("failed to guess item type");
	return (f);
#endif
}

ATI create_ati(THD*& thd, TABLE& tmp_table, Item*& item )
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return ATI(RC_INT, true);
#else
	Field* field = NULL;
	Field* tmp_field = NULL;
	Field* def_field = NULL;
	if(item->type() == Item::FUNC_ITEM) {
		if(item->result_type() != STRING_RESULT)
			field = item->tmp_table_field(&tmp_table);
		else
			field = item->tmp_table_field_from_field_type(&tmp_table, 0);
	} else
		field = create_tmp_field(thd, &tmp_table, item, item->type(), (Item ***) 0, &tmp_field, &def_field, 0, 0, 0, 0, 0);
	if(!field)
		throw DatabaseRCException("failed to guess item type");
	ATI ati = RCEngine::GetCorrespondingATI(*field);
	delete field;
	return ati;
#endif
}

void ResultExportSender::Init(TempTable* t)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	thd->proc_info = "Exporting data";
	DBUG_PRINT("info", ("%s", thd->proc_info));
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(t);

	thd->lex->unit.offset_limit_cnt = 0;

	IOParameters iop;
	BHError bherror;

	export_res->send_fields(fields, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF);

	if((bherror = RCEngine::GetIOParameters(iop, *thd, *export_res->SqlExchange(), 0, true)) != BHERROR_SUCCESS)
		throw LD_Failed;

	List_iterator_fast<Item> li(fields);
	fields_t f;
	li.rewind();
	Item* item(NULL);
	TABLE tmp_table;		// Used during 'Create_field()'
  TABLE_SHARE share;
	init_field_scan_helpers( thd, tmp_table, share );

	std::vector<ATI> deas = t->GetATIs(iop.GetEDF() != TXT_VARIABLE);
	iop.SetNoColumns(uint(deas.size()));
	int i = 0;
	while(item = li++) {
		fields_t::value_type ft = guest_field_type( thd, tmp_table, item );
		f.push_back( ft );
		deas[i] = create_ati(thd, tmp_table, item);
		i++;
	}

	LargeBuffer *lbuf = new LargeBuffer();
	if (!lbuf->IsAllocated()) {
		delete lbuf;
		throw OutOfMemoryRCException("Unable to create Largebuffer.");
	}

	rcbuffer = boost::shared_ptr<LargeBuffer>(lbuf);
	if(!rcbuffer->BufOpen(iop, OVERWRITE))
		throw FileRCException("Unable to open file or named pipe.");

	rcde = DataFormat::GetDataFormat(iop.GetEDF())->CreateDataExporter(iop);
	rcde->Init(rcbuffer, t->GetATIs(iop.GetEDF() != TXT_VARIABLE), f, deas);
#endif
}

void ResultExportSender::SendRecord(TempTable::RecordIterator& iter)		// send to Exporter
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	List_iterator_fast<Item> li(fields);
	Item* l_item;
	li.rewind();
	uint o = 0;
	while(l_item = li++) {
		RCDataType& rcdt((*iter)[o]);
		if(rcdt.IsNull())
			rcde->PutNull();
		else if(ATI::IsTxtType(rcdt.Type())) {
			RCBString val(rcdt.ToRCString());
			if(l_item->field_type() == MYSQL_TYPE_DATE) {
				RCDateTime dt;
				ValueParserForText::ParseDateTime(val,dt,RC_DATE);
				rcde->PutDateTime(dt.GetInt64());
			} else if ((l_item->field_type() == MYSQL_TYPE_DATETIME) || (l_item->field_type() == MYSQL_TYPE_TIMESTAMP)) {
				RCDateTime dt;
				ValueParserForText::ParseDateTime(val,dt,RC_DATETIME);
				if(l_item->field_type() == MYSQL_TYPE_TIMESTAMP) {
					RCDateTime::AdjustTimezone(dt);
				}
				rcde->PutDateTime(dt.GetInt64());
			} else {
				//values from binary columns from TempTable are retrieved as RCBString -> they get RC_STRING type,
				// so an additional check is necessary
				if(dynamic_cast<Item_field*>(l_item) && static_cast<Item_field*>(l_item)->field->binary())
					rcde->PutBin(val);
				else
					rcde->PutText(val);
			}
		} else if(ATI::IsBinType(rcdt.Type()))
			rcde->PutBin(rcdt.ToRCString());
		else if(ATI::IsNumericType(rcdt.Type())) {
			if(rcdt.Type() == RC_BYTEINT)
				rcde->PutNumeric((char)dynamic_cast<RCNum&>(rcdt).Value());
			else if(rcdt.Type() == RC_SMALLINT)
				rcde->PutNumeric((short)dynamic_cast<RCNum&>(rcdt).Value());
			else if(rcdt.Type() == RC_INT || rcdt.Type() == RC_MEDIUMINT)
				rcde->PutNumeric((int)dynamic_cast<RCNum&>(rcdt).Value());
			else
				rcde->PutNumeric(dynamic_cast<RCNum&>(rcdt).Value());
		} else if(ATI::IsDateTimeType(rcdt.Type())) {
			if(rcdt.Type() == RC_TIMESTAMP) {
				// timezone conversion
				RCDateTime& dt(dynamic_cast<RCDateTime&>(rcdt));
				RCDateTime::AdjustTimezone(dt);
				rcde->PutDateTime(dt.GetInt64());
			} else
				rcde->PutDateTime(dynamic_cast<RCDateTime&>(rcdt).GetInt64());
		}
		o++;
	}
	rcde->PutRowEnd();
#endif
}
