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

#ifndef _CQTERM_H_
#define _CQTERM_H_

#include "common/CommonDefinitions.h"
#include "ValueOrNull.h"
#include <vector>

class JustATable;
class ValueSet;
class RCNum;
class RCBString;
class RCDateTime;
class RCDataType;
class QueryOperator;
class VirtualColumn;
class MIUpdatingIterator;

struct AttrID
{
	int n;
	AttrID(): n(NULL_VALUE_32) {}
	explicit AttrID(int _n): n(_n) { }
};

struct TabID
{
	int n;
	TabID(): n(NULL_VALUE_32) {}
	explicit TabID(int _n): n(_n) {}
	bool IsNullID() const {return (n == NULL_VALUE_32); }
	bool operator==(const TabID &other)	const { return (n == other.n) && (! IsNullID()); }
	bool operator<(const TabID &other)	const { return (n < other.n) && (! IsNullID()); }
	bool operator!=(const TabID &other)	const { return !(operator == (other));	}
};

struct CondID	
{ 
	int n; 
	CondID(): n(NULL_VALUE_32) {} 
	explicit CondID(int _n) { n = _n; }
	bool IsNull() const { return (n == NULL_VALUE_32); }
	bool IsInvalid() const { return (n < 0); }
	bool operator==(const CondID& other) const { return (n == other.n) && (! IsNull()); }
};

class MysqlExpression;		// temporary - to be defined externally

enum JoinType		{ JO_INNER, JO_LEFT, JO_RIGHT, JO_FULL };

// DELAYED - one-dimensional condition which must be executed after joins
enum TMParameter	{ TM_DISTINCT, TM_TOP, TM_EXISTS };			// Table Mode Parameter
enum SQType		{ SQ_NONE, SQ_ALL, SQ_ANY };					// Subqueries modifiers
enum CQType		{ CQ_TABLE, CQ_ATTR, CQ_CONST, CQ_OTHER, CQ_NULL, CQ_COMPLEX }; // Type of CQTerm
enum CondType {WHERE_COND, HAVING_COND, ON_INNER_FILTER, ON_LEFT_FILTER, ON_RIGHT_FILTER, OR_SUBTREE, AND_SUBTREE};

/**
  Interpretation of CQTerm depends on which parameters are used.
  All unused parameters must be set to NULL_VALUE (int), NULL (pointers),
  SF_NONE (SimpleFunction), NULL_VALUE_64 (T_int64).
  When these parameters are set:			then the meaning is:
  			attr_id							attribute value of the current table/TempTable
  			attr_id, tab_id					attribute value of a given table/TempTable
 			func, attr_id					attribute value of the current table/TempTable with a simple function applied
 			func, attr_id, tab_id			attribute value of a given table/TempTable with a simple function applied
 			tab_id							value of a TempTable (execution of a subquery)
 			param_id						parameter value
 			c_term							complex expression, defined elsewhere (?)
 	Note: if all above are not used, it is interpreted as a constant (even if null)
 			val_n							numerical (fixed precision) constant
 			val_t							numerical (fixed precision) constant
 * note that all values may be safely copied (pointers are managed outside the class)
 */

struct CQTerm
{
	AttributeType	type;			// type of constant
	VirtualColumn* 	vc;
	int 			vc_id;			// virt column number set at compilation, = -1 for legacy cases (not using virt column)
	bool			is_vc_owner;	// indicator if vc should be it deleted in destructor

	CQTerm(); // null
	explicit CQTerm(int v);
	explicit CQTerm(_int64 v, AttributeType t = RC_INT);
	CQTerm(const CQTerm &);

	~CQTerm();

	bool IsNull() const			{ return (vc_id == NULL_VALUE_32 && vc == NULL); }

	CQTerm & operator=(const CQTerm &);
	bool operator==(const CQTerm &) const;
	char* ToString(char *buf, int tab_id) const;
	char* ToString(char p_buf[], size_t buf_ct, int tab_id) const;
};

#endif
