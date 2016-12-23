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

#ifndef PURE_LIBRARY

#ifndef _RC_ITEMS_H_
#define _RC_ITEMS_H_

#include "common/CommonDefinitions.h"
#include "common/bhassert.h"

class Item_sum_int_rcbase :public Item_sum_num
{
public:
  _int64 count;

  Item_sum_int_rcbase();
  ~Item_sum_int_rcbase();

  longlong val_int();

  double val_real()
  {
	  BHASSERT_WITH_NO_PERFORMANCE_IMPACT(fixed == 1);
	  return (double) val_int();
  }
  String *val_str(String*str);
  my_decimal *val_decimal(my_decimal *);

  enum Sumfunctype sum_func () const { return COUNT_FUNC; }
  enum Item_result result_type () const { return INT_RESULT; }

  void fix_length_and_dec()
  {
	  decimals   = 0;
	  max_length = 21;
	  maybe_null = null_value = 0;
  }

  // void int64_value(__int64 &value);
  void int64_value(_int64 &value);

  void clear();
  mysql_bool add();
  void update_field();
  const char *func_name() const { return "count("; };
};

class Item_sum_sum_rcbase :public Item_sum_num
{
public:
  Item_result hybrid_type;
  double sum;
  my_decimal dec_buffs[1];
  //my_decimal dec_buffs[2];
  //uint curr_dec_buff;
  //void fix_length_and_dec();


  Item_sum_sum_rcbase();
  ~Item_sum_sum_rcbase();


  enum Sumfunctype sum_func () const {return SUM_FUNC;}
  enum Item_result result_type () const { return hybrid_type; }

  double val_real();
  my_decimal* val_decimal(my_decimal*);
  String* val_str(String*);
  longlong val_int();

  my_decimal* dec_value();
  double& real_value();
  void real_value(double&);

  void clear();
  mysql_bool add();
  void update_field();
  //TODO test it more
  #ifdef __GNUC__
  void reset_field(){}
  #else
  void Item_sum_sum_rcbase::reset_field(){}
  #endif

  const char *func_name() const { return "sum("; }

};
//class Item_sum_distinct_rcbase :public Item_sum_num
//{
//public:
//  /* storage for the summation result */
//  ulonglong count;
//  Hybrid_type val;
//
//  enum enum_field_types table_field_type;
//
//  Item_sum_distinct_rcbase();
//  ~Item_sum_distinct_rcbase();
//
//
//  double val_real();
//  my_decimal* val_decimal(my_decimal *);
//  longlong val_int();
//  String* val_str(String *str);
//
//  enum Sumfunctype sum_func () const { return SUM_DISTINCT_FUNC; }
//  enum Item_result result_type () const { return val.traits->type(); }
//
//  my_decimal* dec_value();
//  void real_value(double &value);
//
//  void clear();
//  mysql_bool add();
//  void update_field();
//  const char *func_name() const;
//};

class Item_sum_hybrid_rcbase :public Item_sum
{
protected:
	String tmp_value;
	double sum;
	longlong sum_int;
	my_decimal sum_dec;

//	int cmp_sign;
//	table_map used_table_cache;
	bool was_values;  // Set if we have found at least one row (for max/min only)

public:
	String value;
	enum_field_types hybrid_field_type;
	Item_result hybrid_type;

	Item_sum_hybrid_rcbase();
	~Item_sum_hybrid_rcbase();

	void clear();
	double val_real();
	longlong val_int();
	my_decimal *val_decimal(my_decimal *);
	String *val_str(String *);

	enum Sumfunctype sum_func () const {return MIN_FUNC;}
	enum Item_result result_type () const { return hybrid_type; }
	enum enum_field_types field_type() const { return hybrid_field_type; }
//	void update_field();
//	void min_max_update_str_field();
//	void min_max_update_real_field();
//	void min_max_update_int_field();
//	void min_max_update_decimal_field();
//	void cleanup();
	mysql_bool any_value() { return was_values; }

	my_decimal* dec_value();
	double& real_value();
	_int64& int64_value();
	String* string_value();

	mysql_bool add();
	void update_field();
	void reset_field(){}
	const char *func_name() const { return "min("; }
};

#endif //_RC_ITEMS_H_

#endif
