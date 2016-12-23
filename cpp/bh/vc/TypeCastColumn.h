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

#ifndef TYPECASTCOLUMN_H_
#define TYPECASTCOLUMN_H_

#include "edition/vc/VirtualColumn.h"
#include "core/MIIterator.h"


/*! \brief Converts UTC/GMT time given in seconds since beginning of the EPOCHE to TIME representation
 * \param tmp
 * \param t
 * \return void
 */
extern void GMTSec2GMTTime(MYSQL_TIME* tmp, my_time_t t);
extern bool IsTimeStampZero(MYSQL_TIME& t);

class TypeCastColumn : public VirtualColumn {

public:
	TypeCastColumn(VirtualColumn* from, ColumnType const& to);
	TypeCastColumn(const TypeCastColumn &c);

	const std::vector<VarMap>& GetVarMap() const {return vc->GetVarMap();}
//	void SetParam(VarID var, ValueOrNull val) {vc->SetParam(var, val);}
	void LockSourcePacks(const MIIterator& mit) {vc->LockSourcePacks( mit);}
	void LockSourcePacks(const MIIterator& mit, int th_no);
	void UnlockSourcePacks() {vc->UnlockSourcePacks();}
	void MarkUsedDims(DimensionVector& dims_usage) {vc->MarkUsedDims(dims_usage);}
	dimensions_t GetDimensions() { return vc->GetDimensions();}
	_int64 NoTuples() { return vc->NoTuples(); }
	bool IsConstExpression(MysqlExpression* expr, int temp_table_alias, const std::vector<int>* aliases) {return vc->IsConstExpression(expr, temp_table_alias, aliases);}
	int GetDim() {return vc->GetDim();}
	void RequestEval(const MIIterator& mit, const int tta) {vc->RequestEval(mit, tta);}

	virtual bool IsConst() const { return vc->IsConst(); }
	virtual void GetNotNullValueString(RCBString& s, const MIIterator &m) { return vc->GetNotNullValueString(s, m); }
	virtual _int64 GetNotNullValueInt64(const MIIterator &mit)				{ return vc->GetNotNullValueInt64(mit); }
	virtual bool IsDistinctInTable()				{ return vc->IsDistinctInTable(); }	// sometimes overriden in subclasses
	bool IsDeterministic() { return vc->IsDeterministic();}
	bool IsParameterized() const{ return vc->IsParameterized(); }
	std::vector<VarMap>& GetVarMap() { return vc->GetVarMap(); }
	virtual bool IsTypeCastColumn() const { return true; }
	const MysqlExpression::bhfields_cache_t& GetBHItems() const {return vc->GetBHItems();}

	bool CanCopy() const {return vc->CanCopy();}
	bool IsThreadSafe()  {return vc->IsThreadSafe();}
	std::vector<VirtualColumn*> GetChildren()	{return std::vector<VirtualColumn*>(1,vc);}

protected:
	_int64 DoGetValueInt64(const MIIterator& mit) { return vc->GetValueInt64(mit); }
	bool DoIsNull(const MIIterator& mit) { return vc->IsNull(mit); }
	void DoGetValueString(RCBString& s, const MIIterator& m) { return vc->GetValueString(s, m); }
	double DoGetValueDouble(const MIIterator& m) { return vc->GetValueDouble(m); }
	RCValueObject DoGetValue(const MIIterator& m, bool lookup_to_num) { return vc->GetValue(m, lookup_to_num); }
	_int64 DoGetMinInt64(const MIIterator& m) { return MINUS_INF_64; }
	_int64 DoGetMaxInt64(const MIIterator& m) { return PLUS_INF_64; }
	RCBString DoGetMinString(const MIIterator &m) { return RCBString(); }
	RCBString DoGetMaxString(const MIIterator &m) { return RCBString(); }
	_int64 DoRoughMin() { return MINUS_INF_64 ; }
	_int64 DoRoughMax() { return PLUS_INF_64; }
	_int64 DoGetNoNulls(const MIIterator& m, bool val_nulls_possible) { return vc->GetNoNulls(m); }
	bool DoRoughNullsOnly()	const {return vc->RoughNullsOnly();}
	bool DoNullsPossible(bool val_nulls_possible) { return vc->NullsPossible(); }
	_int64 DoGetSum(const MIIterator& m, bool &nonnegative) { return vc->GetSum(m, nonnegative); }
	bool DoIsDistinct() { return false; }
	_int64 DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind) { return vc->GetApproxDistVals(incl_nulls, rough_mind); }
	ushort DoMaxStringSize() { return vc->MaxStringSize(); }		// maximal byte string length in column
	PackOntologicalStatus DoGetPackOntologicalStatus(const MIIterator& m) { return vc->GetPackOntologicalStatus(m); }
	RSValue DoRoughCheck(const MIIterator& m, Descriptor&d ) { return vc->RoughCheck(m,d); }
	void DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& d) { return vc->EvaluatePack(mit,d); }

	bool full_const;
	VirtualColumn* vc;
};

//////////////////////////////////////////////////////
class String2NumCastColumn : public TypeCastColumn
{
public:
	String2NumCastColumn(VirtualColumn* from, ColumnType const& to);
	_int64 GetNotNullValueInt64(const MIIterator &mit);
	virtual bool IsDistinctInTable()				{ return false; }		// cast may make distinct strings equal
protected:
	_int64 DoGetValueInt64(const MIIterator& mit);
	RCValueObject DoGetValue(const MIIterator&, bool lookup_to_num = true);
	_int64 DoGetMinInt64(const MIIterator& m);
	_int64 DoGetMaxInt64(const MIIterator& m);
	double DoGetValueDouble(const MIIterator& mit);
private:
	mutable _int64 val;
	mutable RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class String2DateTimeCastColumn : public TypeCastColumn
{
public:
	String2DateTimeCastColumn(VirtualColumn* from, ColumnType const& to);
	_int64 GetNotNullValueInt64(const MIIterator &mit);
	virtual bool IsDistinctInTable()				{ return false; }		// cast may make distinct strings equal
protected:
	_int64 DoGetValueInt64(const MIIterator& mit);
	RCValueObject DoGetValue(const MIIterator&, bool lookup_to_num = true);
	void DoGetValueString(RCBString& s, const MIIterator& m);
	_int64 DoGetMinInt64(const MIIterator& m);
	_int64 DoGetMaxInt64(const MIIterator& m);

private:
	_int64 val;
	mutable RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class Num2DateTimeCastColumn : public String2DateTimeCastColumn
{
public:
	Num2DateTimeCastColumn(VirtualColumn* from, ColumnType const& to);
protected:
	_int64 DoGetValueInt64(const MIIterator& mit);
	RCValueObject DoGetValue(const MIIterator&, bool lookup_to_num = true);

private:
	_int64 val;
	mutable RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class DateTime2VarcharCastColumn : public TypeCastColumn
{
public:
	DateTime2VarcharCastColumn(VirtualColumn* from, ColumnType const& to);
protected:
	RCValueObject DoGetValue(const MIIterator&, bool lookup_to_num = true);

private:
	mutable RCValueObject rcv;
};

//////////////////////////////////////////////////////////
class Num2VarcharCastColumn : public TypeCastColumn
{
public:
	Num2VarcharCastColumn(VirtualColumn* from, ColumnType const& to);
	void GetNotNullValueString(RCBString& s, const MIIterator &m) { DoGetValueString(s, m); }
protected:
	RCValueObject DoGetValue(const MIIterator&, bool lookup_to_num = true);
	void DoGetValueString(RCBString& s, const MIIterator& m);
private:
	mutable RCValueObject rcv;
};


//////////////////////////////////////////////////////
class DateTime2NumCastColumn : public TypeCastColumn
{
public:
	DateTime2NumCastColumn(VirtualColumn* from, ColumnType const& to);
	_int64 GetNotNullValueInt64(const MIIterator &mit);
protected:
	_int64 DoGetValueInt64(const MIIterator& mit);
	RCValueObject DoGetValue(const MIIterator&, bool lookup_to_num = true);
	double DoGetValueDouble(const MIIterator& mit);

private:
	mutable _int64 val;
	mutable RCValueObject rcv;
};

//////////////////////////////////////////////////////
class TimeZoneConversionCastColumn : public TypeCastColumn
{
public:
	TimeZoneConversionCastColumn(VirtualColumn* from);
	_int64 GetNotNullValueInt64(const MIIterator &mit);
protected:
	_int64 DoGetValueInt64(const MIIterator& mit);
	RCValueObject DoGetValue(const MIIterator&, bool lookup_to_num = true);
	double DoGetValueDouble(const MIIterator& m);
	void DoGetValueString(RCBString& s, const MIIterator& m);
private:
	mutable _int64 val;
	mutable RCValueObject rcv;
};

class StringCastColumn : public TypeCastColumn
{
public:
	StringCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to) {}
protected:
	void DoGetValueString(RCBString& s, const MIIterator& m) { return vc->GetValueString(s, m); }
	RCValueObject DoGetValue(const MIIterator&, bool lookup_to_num = true);
	ushort DoMaxStringSize() { return ct.GetPrecision(); }
};

#endif /* TYPECASTCOLUMN_H_ */
