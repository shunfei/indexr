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

#include "common/bhassert.h"
#include "compilation_tools.h"
#include "MysqlExpression.h"
#include "types/ValueParserForText.h"
#include "core/RCEngine.h"

using namespace std;

MysqlExpression::MysqlExpression(Item* item, Item2VarID& item2varid, bool use_ibexpr)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	PrintItemTree("MysqlExpression constructed from item tree:", item);

	deterministic = dynamic_cast<Item_func_set_user_var*>(item) ? false : true;
	ib_expression = Query::IBExpressionCheck(item);

	mysql_type = item->result_type();
	if(mysql_type == DECIMAL_RESULT) {		
		uint orig_scale = item->decimals;		
		Query::GetPrecisionScale(item, (int&)decimal_precision, (int&)decimal_scale, true);
		if(orig_scale > decimal_scale) {
			THD& thd = ConnectionInfoOnTLS.Get().Thd();				
			string s = "Precision of an expression result was reduced due to DECIMAL type limitations";
			push_warning(&thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_WARN_DATA_OUT_OF_RANGE, s.c_str());
		}
	} else {
		decimal_precision = 0;
		decimal_scale = 0;
	}

	this->item2varid = &item2varid;
	this->item = TransformTree(item, FORWARD);
	this->item2varid = NULL;
#endif
}

MysqlExpression::~MysqlExpression()
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	TransformTree(item, BACKWARD);
#endif
}

bool MysqlExpression::HandledResultType(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	// Warning: if result type is DECIMAL and precision and/or scale greater than 18,
	// precision/scale of result values will be truncated.
	Item_result type = item->result_type();
	if ( (dynamic_cast<Item_func_get_user_var*>(item)) && type == STRING_RESULT) {
		String s;
		if (item->val_str(&s) == NULL)
			item->max_length = 0;
		else
			item->max_length = (dynamic_cast<Item_func_get_user_var*>(item))->val_str(&s)->length();
	}

	return (type == INT_RESULT) || (type == REAL_RESULT)
		|| ((type == DECIMAL_RESULT) && (item->decimals != NOT_FIXED_DEC))
		|| ((type == STRING_RESULT) && (item->max_length <= 65535));
#endif
}

bool MysqlExpression::HandledFieldType(Item_result type)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	// Here, we don't have to worry about precision/scale of decimals,
	// since all values that come to us were previously stored in BH on int64,
	// so we'll cope with them
	return (type == INT_RESULT) || (type == REAL_RESULT) || (type == DECIMAL_RESULT) || (type == STRING_RESULT);
#endif
}


bool MysqlExpression::SanityAggregationCheck(Item* item, std::set<Item*>& fields, bool toplevel /*= true*/, bool* has_aggregation /*= NULL*/)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	//printItemTree(item);
	if(!item)
		return false;
	if(toplevel && !HandledResultType(item))
		return false;
	if(toplevel && has_aggregation)
		*has_aggregation = false;

	/*
	 * *FIXME*
	 * Item::SUBSELECT_ITEM is not handled here.
	 */
	switch(static_cast<int>(item->type())) {
		case Item::INT_ITEM:
		case Item::REAL_ITEM:
		case Item::STRING_ITEM:
		case Item::DECIMAL_ITEM:
		case Item::NULL_ITEM:
		case Item::VARBIN_ITEM:
			return true;

		case Item_bhfield::BHFIELD_ITEM:
			if(has_aggregation) {
				if(Query::IsAggregationItem(((Item_bhfield*)item)->OriginalItem()))
					*has_aggregation = true;
			}
			fields.insert(((Item_bhfield*)item)->OriginalItem());
			return true;
		case Item::FIELD_ITEM:
			if(((Item_field*)item)->field && !HandledFieldType(item->result_type()))
				return false;
			fields.insert(item);
			return true;
		case Item::FUNC_ITEM: {
			if(dynamic_cast<Item_func_trig_cond*>(item) != NULL)
				return false;

			// currently stored procedures not supported
			if(dynamic_cast<Item_func_sp*>(item) != NULL) {
				Item_func_sp* ifunc = dynamic_cast<Item_func_sp*>(item);
				if(ifunc->argument_count() != 0)
					return false;
				return true;
			}

			// Otherwise, it's a regular function/operator (hopefully)
			Item_func* ifunc = dynamic_cast<Item_func*>(item);
			// xor reports to be Item_func although it is Item_cond
			// therefore we have to go to the next case
			if(strcmp(ifunc->func_name(), "xor") != 0) {

				Item** args = ifunc->arguments();
				bool correct = true;
				for(uint i = 0; i < ifunc->argument_count(); i++) {
					correct = (correct && SanityAggregationCheck(args[i], fields, false, has_aggregation));
					if(!correct)
						break;
				}
				return correct;
			}
		}
		case Item::COND_ITEM: {
			Item_cond* cond = dynamic_cast <Item_cond*> (item);
			List_iterator<Item> li(*cond->argument_list());
			Item* arg;
			bool correct = true;
			while ((arg = li++)) {
				correct = (correct && SanityAggregationCheck(arg, fields, false, has_aggregation));
				if(!correct)
					break;
			}
			return correct;
							  }
		case Item::SUM_FUNC_ITEM: {
			if(!HandledFieldType(item->result_type()))
				return false;
			if(has_aggregation)
				*has_aggregation = true;
			fields.insert(item);
			return true;
		}
		case Item::REF_ITEM: {
			Item_ref* iref = dynamic_cast<Item_ref*>(item);
			if(!iref->ref)
				return false;
			Item* arg = *(iref->ref);
			return SanityAggregationCheck(arg, fields, toplevel, has_aggregation);
		}

		default:
			return false;
	}
#endif
}

Item_bhfield* MysqlExpression::GetBhfieldItem( Item_field* ifield )
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	Item2VarID::const_iterator key = item2varid->find( ifield );
	assert( key != item2varid->end() );
	bhfields_cache_t::iterator it = bhfields_cache.find( key->second );
	Item_bhfield* bhfield = NULL;
	if ( it != bhfields_cache.end() ) {
		bhfield = *it->second.begin();
		bhfield->varID.push_back(key->second);
	} else {
		bhfield = new Item_bhfield(ifield, key->second);
		std::set<Item_bhfield*> s_tmp;
		s_tmp.insert(bhfield);
		bhfields_cache.insert( make_pair( key->second, s_tmp ) );
	}
	return ( bhfield );
#endif
}

void MysqlExpression::RemoveUnusedVarID(Item* root)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return;
#else
	switch(static_cast<int>(root->type())) {
		case Item::FIELD_ITEM:
		case Item::SUM_FUNC_ITEM:
			break;
		case (Item::Type) Item_bhfield::BHFIELD_ITEM: {
				Item_bhfield* bhfield = static_cast<Item_bhfield*>(root);
				bhfield->varID.pop_back();
				assert(bhfield->curr_varID < (int)bhfield->varID.size());
		}
			break;

		case Item::FUNC_ITEM: {
			Item_func* ifunc = static_cast<Item_func*>(root);

			// xor reports to be Item_func although it is Item_cond
			// therefore we have to go to the next case
			if(strcmp(ifunc->func_name(), "xor") != 0) {
				Item** args = ifunc->arguments();
				for(uint i = 0; i < ifunc->argument_count(); i++)
					RemoveUnusedVarID(args[i]);
			}
			break;
		}
		case Item::COND_ITEM: {
			Item_cond* cond = static_cast <Item_cond*> (root);
			List_iterator<Item> li(*cond->argument_list());
			Item* arg;
			while((arg = li++))
				RemoveUnusedVarID(arg);
			break;
		}

		case Item::REF_ITEM: {
    		Item_ref* iref = dynamic_cast<Item_ref*>(root);
    		Item* arg = *(iref->ref);
			RemoveUnusedVarID(arg);
			break;
		}

		case Item::INT_ITEM:
		case Item::REAL_ITEM:
		case Item::STRING_ITEM:
		case Item::DECIMAL_ITEM:
		case Item::NULL_ITEM:
		case Item::VARBIN_ITEM:
			break;

		default:
			BHERROR("Unexpected type of item passed to MysqlExpression::IncrementUsageCounter()");
	}
#endif
}

Item* MysqlExpression::TransformTree(Item* root, TransformDirection dir)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	// Warning: the Item "tree" given by MySQL not necessarily has to be a tree,
	// but instead it can be a DAG (Directed Acyclic Graph).
	// In this case, TransformTree may visit the same nodes several times,
	// so it must be prepared for seeing nodes that are already transformed.
	PrintItemTree("transform tree", root);
	switch(static_cast<int>(root->type())) {
		case Item::FIELD_ITEM: {
			if(dir == BACKWARD)		// already transformed (DAG case)
				return root;
			// dir == FORWARD
			Item_bhfield* bhfield = GetBhfieldItem(static_cast<Item_field*>(root));
			vars.insert(bhfield->varID[0]);

			return bhfield;
		}
		case Item::SUM_FUNC_ITEM: {
				if(dir == BACKWARD)		// already transformed (DAG case)
					return root;
				// dir == FORWARD
				Item_bhfield* bhsum = GetBhfieldItem(static_cast<Item_field*>(root));
				vars.insert(bhsum->varID[0]);
				//Item_bhsum* bhsum = new Item_bhsum(aggregation, (*item2varid)[aggregation]);
				return bhsum;
		}
		case (Item::Type) Item_bhfield::BHFIELD_ITEM: {
			Item_bhfield* bhfield = static_cast<Item_bhfield*>(root);
			if(dir == FORWARD) {		// already transformed (DAG case)
				Item_field* ifield = bhfield->OriginalItem();
				//if(!(bhfield->varID == (*item2varid)[ifield])) {
				//	char err[256];
				//	VarID v1 = bhfield->varID;
				//	VarID v2 = (*item2varid)[ifield];
				//	sprintf(err, "Internal error. The same table field occurs twice in two expressions "
				//			"but in each one has different identifiers assigned: (%d,%d) vs (%d,%d).",
				//			v1.tab, v1.col, v2.tab, v2.col);
				//	//fprintf(stderr, "Error in MysqlExpression::TransformTree():  %s\n", err);
				//	throw RCException(err);
				//}
				std::set<Item_bhfield*> s_tmp;
				s_tmp.insert(bhfield);
				bhfields_cache.insert( make_pair( (*item2varid)[ifield], s_tmp ) );

				vars.insert((*item2varid)[ifield]);
				bhfield->varID.push_back((*item2varid)[ifield]);
				//bhfields_cache.insert(make_pair(bhfield->varID, bhfield));
				return root;
			}
			// dir == BACKWARD
			//delete bhfield;	// done by MySQL not IB, for each Item subclass
			bhfield->ClearBuf();
			return bhfield->OriginalItem();
		}

		case Item::FUNC_ITEM: {
			Item_func* ifunc = static_cast<Item_func*>(root);

			// xor reports to be Item_func although it is Item_cond
			// therefore we have to go to the next case
			if(strcmp(ifunc->func_name(), "xor") != 0) {

				if(dir == FORWARD && (dynamic_cast<Item_func_rand*>(ifunc) || 
					dynamic_cast<Item_func_last_insert_id*>(ifunc) || 
					dynamic_cast<Item_func_get_system_var*>(ifunc) || 
					dynamic_cast<Item_func_is_free_lock*>(ifunc) || 
					dynamic_cast<Item_func_is_used_lock*>(ifunc) || 
					dynamic_cast<Item_func_row_count*>(ifunc) || 
					dynamic_cast<Item_func_sp*>(ifunc)
					/*					// disputable functions start here - should they be nondeterministic?
					|| dynamic_cast<Item_func_weekday*>(ifunc)
					|| dynamic_cast<Item_func_unix_timestamp*>(ifunc)
					|| dynamic_cast<Item_func_time_to_sec*>(ifunc) ||dynamic_cast<Item_date*>(ifunc)
					|| dynamic_cast<Item_func_curtime*>(ifunc) ||dynamic_cast<Item_func_now*>(ifunc)
					|| dynamic_cast<Item_func_microsecond*>(ifunc) ||dynamic_cast<Item_func_last_day*>(ifunc)
					*/					// end of disputable functions
					))
					deterministic = false;

				//			if((dynamic_cast<Item_func_between*>(ifunc))) {
				//				Item_func_between* ifb = dynamic_cast<Item_func_between*>(ifunc);
				//				TransformTree(*ifb->ge_cmp.a,dir);
				//				TransformTree(*ifb->ge_cmp.b,dir);
				//				TransformTree(*ifb->le_cmp.a,dir);
				//				TransformTree(*ifb->le_cmp.b,dir);
				//			}
				Item** args = ifunc->arguments();
				for(uint i = 0; i < ifunc->argument_count(); i++)
					args[i] = TransformTree(args[i], dir);
				return root;
			}
		}

		case Item::COND_ITEM: {
			Item_cond* cond = static_cast <Item_cond*> (root);
			List_iterator<Item> li(*cond->argument_list());
			Item* arg;
			while((arg = li++))
				*(li.ref()) = TransformTree(arg, dir);
			return root;
		}

		case Item::REF_ITEM: {
	    	Item_ref* iref = dynamic_cast<Item_ref*>(root);
	    	Item* arg = *(iref->ref);
    		arg = TransformTree(arg, dir);
    		*(iref->ref)=arg;
    		return root;
		}

		case Item::INT_ITEM:
		case Item::REAL_ITEM:
		case Item::STRING_ITEM:
		case Item::DECIMAL_ITEM:
		case Item::NULL_ITEM:
		case Item::VARBIN_ITEM:
			return root;

		default:
			BHERROR("Unexpected type of item passed to MysqlExpression::TransformTree()");
			return root;
	}
#endif
}

MysqlExpression::SetOfVars& MysqlExpression::GetVars()
{
	return vars;
}

//void MysqlExpression::SetBufsOrParams(BufOfVars* bufs, bool alloc)
//{
//	for ( bhfields_cache_t::iterator it = bhfields_cache.begin(), end = bhfields_cache.end(); it != end; ++ it ) {
//		if(bufs) {
//			BufOfVars::iterator buf = bufs->find(it->first);
//			if(buf != bufs->end())
//				it->second->SetBuf(buf->second, alloc);
////		} else {
////			ValueOrNull* dummy(NULL);
////			it->second->SetBuf(dummy, alloc);
//		}
//	}
//}

void MysqlExpression::SetBufsOrParams(var_buf_t* bufs)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else

	assert(bufs);
	for( MysqlExpression::bhfields_cache_t::iterator it = bhfields_cache.begin(); it != bhfields_cache.end(); ++ it ) {
		MysqlExpression::var_buf_t::iterator buf_set = bufs->find(it->first);
		if(buf_set != bufs->end()) {
			//for each bhitem* in the vector it->second put its buffer to buf_set.second
			for(std::set<Item_bhfield*>::iterator bhfield = it->second.begin(); bhfield != it->second.end(); bhfield++) {
				ValueOrNull* von;
				(*bhfield)->SetBuf(von);
				buf_set->second.push_back(value_or_null_info_t(ValueOrNull(), von));
			}
		}
	}
#endif
}

DataType MysqlExpression::EvalType(TypOfVars* tv)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return DataType(RC_INT);
#else

	// set types of variables (_bhfieldsCache)
	if ( tv ) {
		DataType fieldtype;
		bhfields_cache_t::iterator bhfield_set = bhfields_cache.begin();
		while(bhfield_set != bhfields_cache.end()) {
			MysqlExpression::TypOfVars::iterator it = tv->find(bhfield_set->first);
			if(it != tv->end()) {

				for(std::set<Item_bhfield*>::iterator bhfield = bhfield_set->second.begin(); bhfield != bhfield_set->second.end(); bhfield++) {
					fieldtype = it->second;
					(*bhfield)->SetType(fieldtype);
				}
			}
			bhfield_set++;
		}
	}

	// calculate result type
//	type = DataType();
	switch(mysql_type) {
		case INT_RESULT:		type = DataType(RC_BIGINT); break;
		case REAL_RESULT:		type = DataType(RC_FLOAT); break;
		case DECIMAL_RESULT:	type = DataType(RC_NUM, decimal_precision, decimal_scale); break;
		case STRING_RESULT:		// GA: in case of time item->max_length can contain invalid value 
			if( (item->type() != Item_bhfield::get_bhitem_type() && item->field_type() == MYSQL_TYPE_TIME) ||
				(item->type() == Item_bhfield::get_bhitem_type() && static_cast<Item_bhfield*>(item)->IsAggregation() == false && item->field_type() == MYSQL_TYPE_TIME))
				type = DataType(RC_TIME, 17, 0, item->collation);
			else if( (item->type() != Item_bhfield::get_bhitem_type() && item->field_type() == MYSQL_TYPE_TIMESTAMP) ||
				(item->type() == Item_bhfield::get_bhitem_type() && static_cast<Item_bhfield*>(item)->IsAggregation() == false && item->field_type() == MYSQL_TYPE_TIMESTAMP))
				type = DataType(RC_TIMESTAMP, 17, 0, item->collation);
			else if( (item->type() != Item_bhfield::get_bhitem_type() && item->field_type() == MYSQL_TYPE_DATETIME) ||
				(item->type() == Item_bhfield::get_bhitem_type() && static_cast<Item_bhfield*>(item)->IsAggregation() == false && item->field_type() == MYSQL_TYPE_DATETIME))
				type = DataType(RC_DATETIME, 17, 0, item->collation);
			else if( (item->type() != Item_bhfield::get_bhitem_type() && item->field_type() == MYSQL_TYPE_DATE) ||
				(item->type() == Item_bhfield::get_bhitem_type() && static_cast<Item_bhfield*>(item)->IsAggregation() == false && item->field_type() == MYSQL_TYPE_DATE))
				type = DataType(RC_DATE, 17, 0, item->collation);
			else
				type = DataType(RC_STRING, item->max_length, 0, item->collation);
			// here, it seems that item->max_length
			// stores total length in bytes, not in characters
			// (which are different in case of multi-byte characters)
			break;
		case ROW_RESULT:
			assert(0 && "unexpected type: ROW_RESULT"); break;
	}
	return type;
#endif
}

MysqlExpression::StringType MysqlExpression::GetStringType()
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return STRING_NORMAL;
#else
	if(mysql_type == STRING_RESULT) {
		if( (item->type() != Item_bhfield::get_bhitem_type() && item->field_type() == MYSQL_TYPE_TIME) ||
			(item->type() == Item_bhfield::get_bhitem_type() && static_cast<Item_bhfield*>(item)->IsAggregation() == false && item->field_type() == MYSQL_TYPE_TIME))
			return STRING_TIME;
		else
			return STRING_NORMAL;
	}
	return STRING_NOTSTRING;
#endif
}

//ValueOrNull MysqlExpression::Evaluate()
//{
//#ifdef PURE_LIBRARY
//	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
//	return ValueOrNull();
//#else
//	ValueOrNull val;
//	switch(mysql_type) {
//		case INT_RESULT:
//			{
//				_int64 v = item->val_int();
//				if(v == NULL_VALUE_64)
//					v++;
//				if(v < 0 && item->unsigned_flag)
//					throw NotImplementedRCException("Out of range: unsigned data type is not supported.");
//				val.SetFixed(v);
//				break;
//			}
//
//		case REAL_RESULT:
//			{
//				val = ItemReal2ValueOrNull(item);
//				break;
//			}
//
//		case DECIMAL_RESULT:
//			{
//				val = ItemDecimal2ValueOrNull(item);
//				break;
//			}
//
//		case STRING_RESULT:
//			{
//#ifdef PURE_LIBRARY
//				BHERROR(!"NOT IMPLEMENTED!"); //May work or not - test!
//				String string_result;			
//#endif
//				String* ret = item->val_str(&string_result);
//				if(ret != NULL) {
//					char* p = ret->c_ptr_safe();
//					uint len = ret->length();
//					assert(p[len] == 0);
//
//					if(type.maxstrlen >= 0 && len > uint(type.maxstrlen)) {
//						//fprintf(stderr, "WARNING: maxstrlen=%d of string result of complex expression"
//						//		" is incorrect. Result with length %d occurred\n", type.maxstrlen, len);
//						len = type.maxstrlen;
//						p[len] = 0;
//					}
//
//
//					val.SetString(p, len);
//				}
//				break;
//			}
//		default:
//			assert(0 && "unexpected value");
//	}
//	if(item->null_value)
//		val = ValueOrNull();
//	return val;
//#endif
//}

ValueOrNull MysqlExpression::Evaluate()
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return ValueOrNull();
#else
	switch(mysql_type) {
		case INT_RESULT:
			return ItemInt2ValueOrNull(item);
		case REAL_RESULT:
			return ItemReal2ValueOrNull(item);
		case DECIMAL_RESULT:
			return ItemDecimal2ValueOrNull(item, decimal_scale);
		case STRING_RESULT:
			return ItemString2ValueOrNull(item, type.precision, type.attrtype);
		default:
			assert(0 && "unexpected value");
	}
	return ValueOrNull();
#endif
}

void MysqlExpression::CheckDecimalError(int err)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	// See mysql_source_dir/include/decimal.h for definition of error values (E_DEC_...)
	if(err > E_DEC_TRUNCATED)
		throw RCException(
				"Numeric result of an expression is too large and cannot be handled by Infobright. "
				"Please use an explicit cast to a data type handled by Infobright, "
				"e.g. CAST(<expr> AS DECIMAL(18,6))."
				);
#endif
}

ValueOrNull MysqlExpression::ItemReal2ValueOrNull(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return ValueOrNull();
#else
	ValueOrNull val;
	double v = item->val_real();
	if (v == -0.0)
		v = 0.0;
	_int64 vint = *(_int64*)&v;
	if(vint == NULL_VALUE_64)
		vint++;
	v = *(double*)&vint;
	val.SetDouble(v);
	if(item->null_value)
		val = ValueOrNull();
	return val;
#endif
}

ValueOrNull MysqlExpression::ItemDecimal2ValueOrNull(Item* item, int dec_scale)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return ValueOrNull();
#else
	ValueOrNull val;
	my_decimal dec;
	my_decimal* retdec = item->val_decimal(&dec);
	if(retdec != NULL) {
		if(retdec != &dec)
			my_decimal2decimal(retdec, &dec);
		_int64 v;
		int err;
		//err = my_decimal_shift((uint)-1, &dec, item->decimals <= 18 ? item->decimals : 18);
		if(dec_scale == -1)
			err = my_decimal_shift((uint)-1, &dec, item->decimals <= 18 ? item->decimals : 18);
		else
			err = my_decimal_shift((uint)-1, &dec, dec_scale <= 18 ? dec_scale : 18);
		CheckDecimalError(err);
		err = my_decimal2int((uint)-1, &dec, false, (longlong*) &v);
		CheckDecimalError(err);
		val.SetFixed(v);
	}
	if(item->null_value)
		val = ValueOrNull();
	return val;
#endif
}

ValueOrNull MysqlExpression::ItemString2ValueOrNull(Item* item, int maxstrlen,  AttributeType a_type)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return ValueOrNull();
#else
	ValueOrNull val;
	String string_result;			
	String* ret = item->val_str(&string_result);
	if(ret != NULL) {
		char* p = ret->c_ptr_safe();
		if(ATI::IsDateTimeType(a_type)) {
			RCDateTime rcdt;
			RCBString rbs(p);			
			if (rbs.IsNull())
				return NULL_VALUE_64;
			else {
				BHReturnCode rc = ValueParserForText::ParseDateTime(rbs, rcdt, a_type);
				if(BHReturn::IsError(rc)) {
					return NULL_VALUE_64;
				}
				val.SetFixed(rcdt.GetInt64());
			}
		} else {	
			uint len = ret->length();
			assert(p[len] == 0);
			if(maxstrlen >= 0 && len > uint(maxstrlen)) {
				len = maxstrlen;
				p[len] = 0;
			}
			val.SetString(p, len);
		}
	}
	val.MakeStringOwner();
	if(item->null_value)
		val = ValueOrNull();
	return val;
#endif
}

ValueOrNull MysqlExpression::ItemInt2ValueOrNull(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return ValueOrNull();
#else
	ValueOrNull val;
	_int64 v = item->val_int();
	if(v == NULL_VALUE_64)
		v++;
	if(v < 0 && item->unsigned_flag)
		throw NotImplementedRCException("Out of range: unsigned data type is not supported.");
	val.SetFixed(v);
	if(item->null_value)
		val = ValueOrNull();
	return val;
#endif
}

bool SameBHField(Item_bhfield* const& l, Item_bhfield* const& r)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	return (! (l || r)) ||
			(l && r && ((*l) == (*r)));
#endif

}

bool SameBHFieldSet(MysqlExpression::bhfields_cache_t::value_type const& l, MysqlExpression::bhfields_cache_t::value_type const& r)
{

	return l.second.size() == r.second.size()
			&& equal(l.second.begin(), l.second.end(), r.second.begin(), SameBHField);
}

bool operator == (Item const&, Item const&);

namespace
{

	bool generic_item_same(Item const& l_, Item const& r_)
	{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
		return ( ( &l_ == &r_ )
			|| (
			(l_.type() == r_.type())
			&& (l_.result_type() == r_.result_type())
			&& (l_.cast_to_int_type() == r_.cast_to_int_type())
			&& (l_.string_field_type() == r_.string_field_type())
			&& (l_.field_type() == r_.field_type())
			&& (l_.const_during_execution() == r_.const_during_execution())
			&& ( l_ == r_ )
			)
			);
#endif
	}

}

bool operator == (Item const& l_, Item const& r_)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	Item::Type t = l_.type();
	bool same = t == r_.type();
	if ( same ) {
		switch (static_cast<int>(t)) {
			case (Item::FIELD_ITEM): {
				same = false; // not implemented
//				Item_field const* l = static_cast<Item_field const*>(&l_);
//				Item_field const* r = static_cast<Item_field const*>(&r_);
	//					same = l->field->
			} break;
			case (Item::COND_ITEM):
			case (Item::FUNC_ITEM): {
				Item_func const* l = static_cast<Item_func const*>(&l_);
				Item_func const* r = static_cast<Item_func const*>(&r_);
				same = ! strcmp( l->func_name(), r->func_name() );
				same = same && ( l->arg_count == r->arg_count);
				same = same && l->functype() == r->functype();
				if(l->functype() == Item_func::GUSERVAR_FUNC) {
					if ( same ) {
						Item_func_get_user_var const* ll = static_cast<Item_func_get_user_var const*>(&l_);
						Item_func_get_user_var const* rr = static_cast<Item_func_get_user_var const*>(&r_);
						same = ! strcmp(ll->name.str, rr->name.str);
					}
				} else {
					same = same && l->arg_count == r->arg_count;
					for(uint i = 0; same && (i < l->arg_count); ++ i)
						same = same && (*l->arguments()[i] == *r->arguments()[i]);

					//Item_func* lll = (Item_func*)&l;
					//Item_func* mmm = (Item_func*)&r;

					//bool x = l->const_item();
					//bool y = r->const_item();
					//longlong zzz = lll->val_int_result();
					//longlong vvv = mmm->val_int_result();
					same = same && (l->const_item() == r->const_item());
					if(same && l->const_item())
						same = ((Item_func*)&l_)->val_int() == ((Item_func*)&r_)->val_int();
					if(dynamic_cast<const Item_date_add_interval*>(&l_)) {
						const Item_date_add_interval* l= static_cast<const Item_date_add_interval*>(&l_);
						const Item_date_add_interval* r= static_cast<const Item_date_add_interval*>(&r_);
						same = same && dynamic_cast<const Item_date_add_interval*>(&r_);
						same = same && ((l->int_type == r->int_type) && (l->date_sub_interval == r->date_sub_interval));
					}
					if(l->functype() == Item_func::IN_FUNC) {
						const Item_func_in* l = static_cast<const Item_func_in*>(&l_);
						const Item_func_in* r = static_cast<const Item_func_in*>(&r_);
						same = same && l->negated == r->negated;
					}
					if(l->functype() == Item_func::COND_AND_FUNC || l->functype() == Item_func::COND_OR_FUNC || l->functype() == Item_func::COND_XOR_FUNC ) {
						Item_cond* l =  const_cast<Item_cond*>(static_cast<Item_cond const*>(&l_));
						Item_cond* r =  const_cast<Item_cond*>(static_cast<Item_cond const*>(&r_));
						List_iterator<Item> li(*l->argument_list());
						List_iterator<Item> ri(*r->argument_list());
						Item* il, *ir;
						while((il=li++) && (ir=ri++)) {
							same = same && *il == *ir;
						}
						same = same && (!ir && !il);
					}
				}
			} break;
			case (Item_bhfield::BHFIELD_ITEM) : {
				Item_bhfield const* l = static_cast<Item_bhfield const*>(&l_);
				Item_bhfield const* r = static_cast<Item_bhfield const*>(&r_);
				same = (*l == *r);
			} break;
			case (Item::REF_ITEM ): {
				Item_ref const* l = static_cast<Item_ref const*>(&l_);
				Item_ref const* r = static_cast<Item_ref const*>(&r_);
				same = ( ! ( l->ref || r->ref ) )
					|| ( l->ref && r->ref
							&& ( ( ! ( *(l->ref) || *(r->ref) ) ) || ( *(l->ref) && *(r->ref ) && ( *(*(l->ref)) == *(*(r->ref)) ) ) ) );
			} break;
			case (Item::NULL_ITEM):
			case (Item::STRING_ITEM):
			case (Item::DECIMAL_ITEM):
			case (Item::REAL_ITEM):
			case (Item::VARBIN_ITEM):
			case (Item::INT_ITEM): {
				same = l_.eq(&r_, true);
			} break;
			default: {
				same = generic_item_same( l_, r_ );
			} break;
		}
	}
	return (same);
#endif
}

bool MysqlExpression::operator == (MysqlExpression const& other) const
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	return (
		(mysql_type == other.mysql_type)
		&& (decimal_precision == other.decimal_precision)
		&& (decimal_scale == other.decimal_scale)
		&& (deterministic == other.deterministic)
		&& (*item == *(other.item))
		&& (bhfields_cache.size() == other.bhfields_cache.size())
		&& vars == other.vars
		&& equal(bhfields_cache.begin(), bhfields_cache.end(), other.bhfields_cache.begin(), SameBHFieldSet)
		);
#endif
}
