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


#ifndef VIRTUALCOLUMNBASE_H_INCLUDED
#define VIRTUALCOLUMNBASE_H_INCLUDED

#include <vector>

#include "core/Column.h"
#include "core/MultiIndex.h"
#include "core/PackGuardian.h"
#include "core/MysqlExpression.h"
#include "core/JustATable.h"
#include "util/DestructionDebugger.h"

class MIIterator;
class RoughMultiIndex;

/*! \brief A column defined by an expression (including a subquery) or encapsulating a PhysicalColumn
 * VirtualColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in VirtualColumn object may not exist physically, they can be computed on demand.
 *
 */

class VirtualColumnBase : public Column DEBUG_STATEMENT( DEBUG_COMA public DestructionDebugger<VirtualColumnBase> )
{
public:
	typedef std::set<int> dimensions_t;
	struct VarMap {
		VarMap(VarID v, JustATable* t, int d)
			:	var(v), col_ndx( v.col < 0 ? -v.col - 1 : v.col), dim(d), tab(t->shared_from_this())
		{}
		VarID			var;		//variable identified by table alias and column number
		int 			col_ndx;	//column index (not negative)
		int				dim;		//dimension in multiindex corresponding to the alias
		JustATable*		tabp;

		JustATable* GetTabP()		{return tabp;}
		JustATablePtr	GetTabPtr() const {	return tab.lock(); }

		bool operator < ( VarMap const& v ) const {
			return ( var < v.var )
				|| ( ( var == v.var ) && ( col_ndx < v.col_ndx ) )
				|| ( ( var == v.var ) && ( col_ndx == v.col_ndx ) && ( GetTabPtr() < v.GetTabPtr() ) )
				|| ( ( var == v.var ) && ( col_ndx == v.col_ndx ) && ( GetTabPtr() == v.GetTabPtr() ) && ( dim < v.dim ) );
		}
		JustATableWeakPtr		tab;		//table corresponding to the alias
	};

	/*! \brief Create a virtual column for a given column type.
	 *
	 * \param col_type the column to be "wrapped" by VirtualColumn
	 * \param mind the multiindex to which the VirtualColumn is attached.
	 */
	VirtualColumnBase(ColumnType const& col_type, MultiIndex* mind);
	VirtualColumnBase(VirtualColumn const& vc);

	typedef enum {SC_NOT = 0, SC_ATTR, SC_RCATTR} single_col_t;

	virtual ~VirtualColumnBase() {}; 

	/////////////// Data access //////////////////////

	/*! \brief Get a numeric value from a column
	 *
	 * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and locked
	 *
	 * Use it for numeric columns only.
	 * Old name: \b GetTable64
	 *
	 * \param mit points to a row in an Multiindex, requested column value comes from this row
	 * \return numeric value from the column in level-1 encoding.
	 * - Integers values can be used directly
	 * - Decimals must be externally properly shifted
	 * - Doubles must be cast e.g. *(double*)&
	 */
	inline _int64 GetValueInt64(const MIIterator &mit) {
		return DoGetValueInt64(mit);
	}
	virtual _int64 GetNotNullValueInt64(const MIIterator &mit) = 0;

	/*! \brief Is the column value NULL ?
	 *
	 * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and locked
	 *
	 * \param mit points to a row in an Multiindex, requested column value comes from this row
	 * \return \b true if column value is NULL, \b false otherwise
	 */
	inline bool IsNull(const MIIterator &mit) {
		return DoIsNull(mit);
	}

	/*! \brief Get a non null-terminated String from a column
	 *
	 * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and locked
	 *
	 * Can be used to retrieve binary data
	 *
	 * \param s a string object to be filled with a retrieved value
	 * \param mit points to a row in an Multiindex, requested column value comes from this row
	 * \param bin_as_hex in case of a binary column, present the value as hexadecimal number
	 */
	inline void GetValueString(RCBString& s, const MIIterator &mit) {
		DoGetValueString(s, mit);
	}
	virtual void GetNotNullValueString(RCBString& s, const MIIterator &mit) = 0;


	/*! \brief Get a double value from a column, possibly converting the original value
	 *
	 * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and locked
	 *
	 * \param mit points to a row in an Multiindex, requested column value comes from this row
	 */
	inline double GetValueDouble(const MIIterator& mit) {
		return (DoGetValueDouble(mit));
	}

	/*! \brief Get a value from a colunm, whatever the column type is
	 *
	 * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and locked
	 *
	 * \param mit points to a row in an Multiindex, requested column value comes from this row
	 * \param lookup_to_num legacy meaning (?)
	 */
	inline RCValueObject GetValue(const MIIterator& mit, bool lookup_to_num = false) {
		return (DoGetValue(mit, lookup_to_num));
	}

	/////////////// Statistics //////////////////////

	/*! \brief Get rough minimum for packrow in level-1 encoding
	 *
	 * \param mit iterator pointing to a packrow, for which min value is requested
	 * \return numeric value from the column in level-1 encoding. or MINUS_INF_64 if no min available
	 * \return undefined for nulls only or empty pack
	 * - Integers values can be used directly
	 * - Decimals must be externally properly shifted
	 * - Doubles must be cast e.g. *(double*)&
	 */
	_int64 GetMinInt64(const MIIterator& mit);

	/*! \brief Get minimum for packrow in level-1 encoding
	 * 	\return min or NULL_VALUE_64 if min not known exactly (e.g. pack not full)
	 */
	inline _int64 GetMinInt64Exact(const MIIterator& mit) {
		return DoGetMinInt64Exact(mit);
	}

	/*! \brief Get rough maximum for packrow in level-1 encoding
	 *
	 * Old name: GetMax64
	 * \param mit iterator pointing to a packrow, for which max value is requested
	 * \return numeric value from the column in level-1 encoding. or PLUS_INF_64 if no max available
	 * \return undefined for nulls only or empty pack
	 * - Integers values can be used directly
	 * - Decimals must be externally properly shifted
	 * - Doubles must be cast e.g. *(double*)&
	 */
	_int64 GetMaxInt64(const MIIterator& mit);

	/*! \brief Get maximum for packrow in level-1 encoding
	 * 	\return max or NULL_VALUE_64 if max not known exactly (e.g. pack not full)
	 */
	inline _int64 GetMaxInt64Exact(const MIIterator& mit) {
		return DoGetMaxInt64Exact(mit);
	}

	/*! \brief Get rough maximum for packrow in level-1 encoding
	 *
	 * \param mit iterator pointing to a packrow, for which max value is requested
	 * \return string value from the column in level-1 encoding or RCBString.IsNULL() if max not available
	 */
	inline RCBString GetMaxString(const MIIterator& mit) {
		return DoGetMaxString(mit);
	}

	/*! \brief Get rough minimum for packrow in level-1 encoding
	 *
	 * \param mit iterator pointing to a packrow, for which min value is requested
	 * \return string value from the column in level-1 encoding or RCBString.IsNULL() if min not available
	 */
	inline RCBString GetMinString(const MIIterator& mit) {
		return DoGetMinString(mit);
	}

	/*! \brief Get (global) rough minimum for numerical column in level-1 encoding
	 *  For floating point types it is a double min encoded on _int64
	 *  Special values: MINUS_INF_64 is default (also for double), undefined for nulls only or empty set
	 */
	_int64 RoughMin();

	/*! \brief Get (global) rough maximum for numerical column in level-1 encoding
	 *  For floating point types it is a double max encoded on _int64
	 *  Special values: PLUS_INF_64 is default (also for double), undefined for nulls only or empty set
	 */
	_int64 RoughMax();

	/*! \brief A coefficient of rough check selectivity for simple equality conditions.
	 *  \return Approximation of k/n, where k - no. of suspected data packs, n - no. of all data packs.
	 *  E.g. 1 means KNs are useless, 1/n means only one pack is to be checked (like for "a = 100" for autoincrement).
	 */
	virtual double RoughSelectivity()
	{ return 1; }

	/*! \brief Update the given TextStat to be able to encode string values from the column. Invalidate s if not possible.
	*/
	virtual void GetTextStat(TextStat &s)
	{ s.Invalidate(); }

	/*!
	 * \brief Get number of NULLs in the packrow pointed by \e mit
	 * \pre \e mit is located at the beginning of current packrow
	 * \return number of nulls in the pack or NULL_VALUE_64 if not available
	 * Note that int may be not enough, as MIIterator packrow may be arbitrarily large.
	 */
	inline _int64 GetNoNulls(const MIIterator& mit) {
		return DoGetNoNulls(mit, vc_nulls_possible);
	}

	/*!
	 * \brief returns true -> column contains only NULLs for sure
	 * \brief returns false -> it is sure that column contains only NULLs
	 * If returns true, rough level Min/Max methods should not be used for this column
	 */
	inline bool RoughNullsOnly() const {
		return DoRoughNullsOnly();
	}

	/*!
	 * \brief Return true if it is possible that the column will return null value,
	 * including nulls generated by multiindex null objects
	 */
	inline bool NullsPossible() {
		return DoNullsPossible(vc_nulls_possible);
	}

	/*! \brief Get the sum of values in the packrow pointed by \e mit
	 * \pre \e mit is located at the beginning of current packrow
	 * \return sum or NULL_VALUE_64 if sum not available (e.g. if the packrow is not full), set nonnegative as true if
	 *         the sum is based on nonnegative values only (=> the sum is an upper approx. of a sum of a subset)
	 */
	inline _int64 GetSum(const MIIterator &mit, bool &nonnegative) {
		return DoGetSum(mit, nonnegative);
	}

	/*! \brief Get an upper approximation of the sum of the whole packrow pointed by \e mit (even if the iterator is not full for the pack)
	 * \pre \e mit is located at the beginning of current packrow
	 * \return sum or NULL_VALUE_64 if sum not available, set nonnegative as true if
	 *         the sum is based on nonnegative values only (=> the sum is an upper approx. of a sum of a subset)
	 */
	inline _int64 GetApproxSum(const MIIterator &mit, bool &nonnegative) {
		return DoGetApproxSum(mit, nonnegative);
	}

	/*! \brief Return true if the virtual column contain only non-null distinct values
	 *  Depends on the current multiindex state, e.g. if there is no row repetitions.
	 */
	inline bool IsDistinct() {
		return DoIsDistinct();
	}

	/*! \brief Return true if the virtual column is based on single physical column and contain distinct values within its table
	*/
	virtual bool IsDistinctInTable()							{ return IsDistinct(); }

	/*! \brief Return the best possible upper approximation of distinct values, incl. nulls (or not)
	 *  Depends on the current multiindex state, e.g. KNs, if there are nulls etc.
	 */
	_int64 GetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind = NULL);

	/*! \brief Return the exact number of distinct values of the whole column, without nulls.
	 *  If the exact number is unsure, return NULL_VALUE_64.
	 */
	virtual _int64 GetExactDistVals() {
		return NULL_VALUE_64;
	}

	/*! \brief Return the upper approximation of text size the column may return.
	 *  May depend on the current multiindex state, e.g. KNs.
	 *  Used e.g. to prepare buffers for text values.
	 *  maximal byte string length in column
	 */
	inline ushort MaxStringSize() {
		ushort res = DoMaxStringSize();
		if(int(res) > vc_max_len)
			return ushort(vc_max_len);
		return res;
	}

	virtual RCBString	DecodeValue_S(_int64 code)	{ assert(0); return RCBString(); }	// lookup (physical) only
	virtual int			EncodeValue_S(RCBString &v)	{ assert(0); return -1; }	// lookup (physical) only
	_int64 DecodeValueAsDouble(_int64 code);		// convert code to a double value and return as binary-encoded
	virtual std::vector<_int64> GetListOfDistinctValues(MIIterator const& mit)
	{ std::vector<_int64> empty; return empty; }

	////////////////////////////////////////////////////////////

	/*! Get list of variables which must be supplied as arguments the obtain a value from this VirtualColumn
	 * It is a list of variables from underlying MysqlExpression, or a the PhysicalColumn if VirtualColumn wraps a PhysicalColumn
	 *
	 */
	virtual std::vector<VarMap>& GetVarMap() {return var_map;}

	/*! Get list of parameters which values must be set before requesting any value from this VirtualColumn
	 * It is a list of variables from underlying MysqlExpression, which do not belong to tables from teh current query
	 *
	 */
	virtual const  MysqlExpression::SetOfVars& GetParams() const {return params;}

	//! \brief parameters were set, so reevaluation is necessary
	//! \brief for IBConstExpressions, get arguments according to mit and evaluate
	//! the evaluation in IBConstExpression i done only if requested by TempTable identified by tta
	virtual void RequestEval(const MIIterator& mit, const int tta) {first_eval = true;}

	//! \return true, if \e d2 is no longer needed
	virtual bool TryToMerge(Descriptor &d1,Descriptor &d2) { return false; }	// default behaviour, should be overriden

	//! \brief Assign a value to a parameter
	//! It is up the to column user to set values of all the parameters before requesting any value from the column
	virtual void SetParamTypes(MysqlExpression::TypOfVars* types) {};

	/*! Mark as true each dimension used as source for variables in this VirtualColumn
	 *
	 * \param dims_usage caller creates this vector with the proper size (number of dimensions in the multiindex)
	 */
	virtual void MarkUsedDims(DimensionVector& dims_usage);

	/*! Check the condition \e desc on a packrow from \e mind pointed by \e mit
	 *
	 * Does not perform a rough check - does row by row comparison
	 *
	 * \pre Assumes that triviality of desc is checked beforehand.
	 * Requires a new MultiIndex functionality:
	 * Tell, that changes to the current packrow are finished
	 * void MultiIndex::CommitPendingPackUpdate();
	 * Apply all pending changes to the MutliIndex, so they become visible
	 * void MultiIndex::CommitPendingUpdate();
	 */
	void EvaluatePack(MIUpdatingIterator& mit, Descriptor& desc) {
		DoEvaluatePack(mit, desc);
	}

	//! check whether any value from the pack may meet the condition
	inline RSValue RoughCheck(const MIIterator& it, Descriptor& d) {
		return DoRoughCheck(it, d);
	}

	//! is a datapack pointed by \e mit NULLS_ONLY, UNIFORM, UNIFORM_AND_NULLS or NORMAL
	PackOntologicalStatus GetPackOntologicalStatus(const MIIterator& mit) {
		return DoGetPackOntologicalStatus(mit);
	}

	/*! \brief Lock datapacks providing parameters for contained expression; must be done before any data access.
	 *  \param mit Multiindex iterator pointing to a packrow to be locked
	 *  Lock strategy is controlled by an internal PackGuardian.
	 *  Default strategy: unlocking the pack when the next one is locked.
	 * \code // This is an example of use LockSourcePacks to list all values of VirtualColumn:
	 *       MIIterator it(current_multiindex);
	 *       while(it.IsValid()) {
	 *           if(it.PackrowStarted()) {
	 *               virt_column.LockSourcePack(it);
	 *           }
	 *           cout << virt_column.GetValueInt64(it) << endl;
	 *           ++it;
	 *       }
	 *       // may be unlocked manually, or just left until destructor does it
	 */
	virtual void LockSourcePacks(const MIIterator& mit) {}

	//! \brief Manually unlock all locked datapacks providing parameters for contained expression.
	//! Done also in destructor.
	virtual void UnlockSourcePacks() {}
	virtual void DisplayAttrStats()		{}

	/*! \brief Returns true if access to column values does not depend on MultiIndexIterator, false otherwise
	 * \return bool
	 */
	virtual bool IsConst() const = 0;

	/*! \brief Returns not zero if the virtual column is an interface to a single physical column, otherwise SC_ATTR or SC_RCATTR
	* \return single_col_t
	*/
	virtual single_col_t IsSingleColumn() const { return SC_NOT; }

	/*! \brief Returns true if the virtual column is a set (multival), false otherwise
	* \return bool
	*/
	virtual bool IsMultival() const { return false; }

	/*! \brief Returns true if the virtual column is a subselect column, false otherwise
	* \return bool
	*/
	virtual bool IsSubSelect() const { return false; }

	/*! \brief Returns true if the virtual column is a TypeCast column, false otherwise
	* \return bool
	*/
	virtual bool IsTypeCastColumn() const { return false; }

	/*! \brief Returns true if the virtual column is an InSetColumn, false otherwise
	* \return bool
	*/
	virtual bool IsInSet() const { return false; }

	/*! \brief Returns true if the virtual column is an IBExpressionColumn, false otherwise
	* \return bool
	*/
	virtual bool IsIBExpression() const { return false; }

	/*! \brief Returns true if column values are deterministically set, false otherwise
	 * \return bool
	 * note - not properly implemented in MultivalueColumns.
	 */
	virtual bool IsDeterministic() { return true;}

	/*! \brief Returns true if column has parameters that have to be set before accessing its values, false otherwise
	 * \return bool
	 */
	virtual bool IsParameterized() const { return params.size() != 0; }

	/*! Is there a procedure to copy the column, so the copies can be used in parallel
	 *
	 */
	virtual bool CanCopy() const {return false;}

	/*! Is there a procedure to copy the column, so the copies can be used in parallel
	 *
	 */
	virtual VirtualColumn* Copy() {assert(0 && "not implemented"); return NULL;}

	virtual void AssignFromACopy(const VirtualColumn* vc);

	virtual bool IsThreadSafe() {return false;}

	/*! \brief Returns all dimensions (tables) involved by virtual column
	 *
	 */
	virtual dimensions_t GetDimensions();

	/*! \brief Returns the only dimension (table) involved by virtual column, or -1
	 *
	 */
	virtual int GetDim() {return dim;}

	ConnectionInfo& ConnInfo()		{ return conn_info; }
	MultiIndex* 	GetMultiIndex()	{ return mind; }

	/*! change the MultiIndex for which the column has been created, used in subquery execution
	 * \param m - new MultiIndex, should be a copy of the previously assigned one
	 * \param t - this copy of original (compilation-produced) TempTable must be used by the column to take values from
	 */
	void 			SetMultiIndex(MultiIndex* m, JustATablePtr t = JustATablePtr());

	virtual _int64 NoTuples(void);

	//! Is it a CONST column without parameters? (i.e. fully defined by a value)
	bool IsFullConst() { return IsConst() && !IsParameterized(); }

	static bool IsConstExpression(MysqlExpression* expr, int temp_table_alias, const std::vector<int>* aliases);

	virtual const MysqlExpression::bhfields_cache_t& GetBHItems() const = 0;

	virtual char *ToString(char p_buf[], size_t buf_ct) const	{ return p_buf; }

	typedef std::vector<VarMap> var_maps_t;

	/*! Must be called in constructor, and every time the VirtualColumn looses its history
	*/
	void ResetLocalStatistics();

	/*! \brief Set local statistics for a virtual column. Use NULL_VALUE_64 or MINUS_INF_64/PLUS_INF_64 to leave min or max unchanged.
	 * Note that this function may only make the min/max more narrow, i.e. settings wider than current min/max will be ignored.
	 * Use ResetLocalStatistics() first to make the interval wider.
	*/
	void SetLocalMinMax(_int64 loc_min, _int64 loc_max);

	/*! \brief Set local null information for a virtual column. Override the old value.
	*/
	void SetLocalNullsPossible(bool loc_nulls_possible)				{ vc_nulls_possible = loc_nulls_possible; }

	/*! \brief Set max. number of not null distinct values.
	*   Decreases the old value. To set it unconditionally, set NULL_VALUE_64 first.
	*/
	void SetLocalDistVals(_int64 loc_dist_vals)						
	{ 
		if(vc_dist_vals == NULL_VALUE_64 || vc_dist_vals > loc_dist_vals)
			vc_dist_vals = loc_dist_vals; 
	}
	void SetLocalMaxLen(int loc_max_len)							{ vc_max_len = loc_max_len; }

	void SetLocalNullsOnly(bool loc_nulls_only)				{ nulls_only = loc_nulls_only; }
	bool GetLocalNullsOnly()								{ return nulls_only; }

	/*! returns true if the column can be modified during subquery execution, so a subquery
	 *  must work on a copy
	 */
	virtual bool MustCopyForSubq()									{ return false;}

	//! \brief true if method Prepare() should be issued before the column is used in a query
	virtual bool NeedsPreparing() const								{return false;}
	virtual void Prepare()											{}

	virtual std::vector<VirtualColumn*> GetChildren()		{return std::vector<VirtualColumn*>();}
protected:
	virtual _int64 DoGetValueInt64(const MIIterator&) = 0;
	virtual bool DoIsNull(const MIIterator&) = 0;
	virtual void DoGetValueString(RCBString&, const MIIterator&) = 0;
	virtual double DoGetValueDouble(const MIIterator&) = 0;
	virtual RCValueObject DoGetValue(const MIIterator&, bool) = 0;
	virtual _int64 DoGetMinInt64(const MIIterator &) = 0;
	virtual _int64 DoGetMinInt64Exact(const MIIterator &) {return NULL_VALUE_64;}
	virtual _int64 DoGetMaxInt64Exact(const MIIterator &) {return NULL_VALUE_64;}
	virtual _int64 DoGetMaxInt64(const MIIterator &) = 0;
	virtual RCBString DoGetMinString(const MIIterator &) = 0;
	virtual RCBString DoGetMaxString(const MIIterator &) = 0;
	virtual _int64 DoRoughMin() = 0;
	virtual _int64 DoRoughMax() = 0;
	virtual _int64 DoGetNoNulls(const MIIterator&, bool val_nulls_possible) = 0;
	virtual bool DoRoughNullsOnly() const = 0;
	virtual bool DoNullsPossible(bool val_nulls_possible) = 0;
	virtual _int64 DoGetSum(const MIIterator&, bool &nonnegative) = 0;
	virtual _int64 DoGetApproxSum(const MIIterator& mit, bool &nonnegative);
	virtual bool DoIsDistinct() = 0;
	virtual _int64 DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind) = 0;
	virtual ushort DoMaxStringSize() = 0;		// maximal byte string length in column
	virtual PackOntologicalStatus DoGetPackOntologicalStatus(const MIIterator&) = 0;
	virtual RSValue DoRoughCheck(const MIIterator&, Descriptor&);
	virtual void DoEvaluatePack(MIUpdatingIterator& mit, Descriptor&) = 0;

protected:
	MultiIndex* mind;
	ConnectionInfo& conn_info;
	var_maps_t var_map;
	MysqlExpression::SetOfVars params; //parameters - columns from tables not present in the local multiindex

	mutable ValueOrNull last_val;
	//! for caching, the first time evaluation is obligatory e.g. because a parameter has been changed

	mutable bool first_eval;

	int dim;						//valid only for SingleColumn and for ExpressionColumn if based on a single table only
									//then this an easily accessible copy of var_map[0]._dim. Otherwise it should be -1

	//////////// Local statistics (set by history) ///////////////
	_int64	vc_min_val;					// _int64 for ints/dec, *(double*)& for floats
	_int64	vc_max_val;
	bool	vc_nulls_possible;			// false => no nulls, except of outer join ones (to be checked in multiindex)
	_int64	vc_dist_vals;				// upper approximation of non-null distinct vals, or NULL_VALUE_64 for no approximation
	int		vc_max_len;					// max string length
	bool	nulls_only;					// only nulls are present
};

#endif /* not VIRTUALCOLUMNBASE_H_INCLUDED */

