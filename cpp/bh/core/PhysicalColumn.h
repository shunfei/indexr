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


#include "Column.h"
#include "MysqlExpression.h"
#include "Descriptor.h"
#include "CompiledQuery.h"

#ifndef PHYSICALCOLUMN_H_
#define PHYSICALCOLUMN_H_

//! A column in a table. Values contained in it exist physically on disk and/or in memory and
//! are divided into datapacks
class PhysicalColumn : public Column
{
public:
	PhysicalColumn(): name(0), desc(0), is_unique(false), is_unique_updated(false) {}
	PhysicalColumn(const PhysicalColumn& phc) : Column(phc), name(0), desc(0),
	is_unique(phc.is_unique), is_unique_updated(phc.is_unique_updated) {}

	enum phys_col_t {ATTR, RCATTR};

	/*! \brief Get a numeric value from a column
	 *
	 * \pre necessary datapacks are loaded and locked. TODO: Currently dps may be not loaded
	 *
	 * Use it for numeric columns only.
	 * Old name: \b GetTable64
	 *
	 * \param row identifies a row
	 * \return numeric value from the column in level-1 encoding.
	 * - Integers values can be used directly
	 * - Decimals must be externally properly shifted
	 * - Doubles must be cast e.g. *(double*)&
	 */
	virtual _int64 GetValueInt64(_int64 row) const = 0;
	virtual _int64 GetNotNullValueInt64(_int64 row) const = 0;

	/*! \brief Is the column value NULL ?
	 *
	 * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and locked
	 *
	 * \param row identifies a row
	 * \return \b true if column value is NULL, \b false otherwise
	 */
	virtual bool IsNull(_int64 row) const = 0;

	/*! \brief Get a numeric value from a column
		 *
		 * \pre necessary datapacks are loaded and locked
		 *
		 * \param row identifies a row
		 * \return RCNum object - a representation of a numeric value
		 */
//	virtual const RCNum& GetValueNum(const _uint64 row) const
//               {BHASSERT(false, "not overridden"); static RCNum dummy(0.0); return dummy; };

	/*! \brief Get a date/time value from a column
		 *
		 * \pre necessary datapacks are loaded and locked
		 *
		 * \param row identifies a row
		 * \return RCDateTime object - a representation of a date/time value
		 */
//	virtual const RCDateTime& GetValueDateTime(const _uint64 row) const
//               {BHASSERT(false, "not overridden"); static RCDateTime dummy(0); return dummy; };

	/*! \brief Get a binary value from a column
		 *
		 * \pre necessary datapacks are loaded and locked
		 *
		 * \param row identifies a row
		 * \param size the size of buffer, set to the number of copied bytes on return
		 * \param buf buffer into which the bytes are copied
		 */
	virtual void GetValueString(_int64 row, RCBString& s) = 0;
	virtual void GetNotNullValueString(_int64 row, RCBString& s) = 0;
	virtual RCValueObject GetValue(_int64 obj, bool lookup_to_num = false) = 0;

//ToDO uncomment and solve cyclic includes
//	virtual MysqlExpression::ValueOrNull GetComplexValue(const _uint64 obj);

	/*! \brief Get a text value from a column
		 *
		 * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and locked
		 *
		 * \param row is a row number in the column
		 * \param size the size of buffer, set to the number of copied bytes on return
		 * \param buf buffer into which the bytes are copied
		 * \return number of characters put in the buffer
		 */
//	virtual int	GetValueText(_uint64 obj, char *val_buf) const = 0;


	/*! \brief Get rough minimum in level-1 encoding
		 *
		 * Old name: GetMin64
		 * \param pack identifies a datapck, for which min value is requested
		 * \return numeric value from the column in level-1 encoding.
		 * - Integers values can be used directly
		 * - Decimals must be externally properly shifted
		 * - Doubles must be cast e.g. *(double*)&
		 */
	virtual _int64 GetMinInt64(int pack) = 0;

	/*! \brief Get rough maximum in level-1 encoding
		 *
		 * Old name: GetMax64
		 * \param pack identifies a datapck, for which min value is requested
		 * \return numeric value from the column in level-1 encoding.
		 * - Integers values can be used directly
		 * - Decimals must be externally properly shifted
		 * - Doubles must be cast e.g. *(double*)&
		 */
	virtual _int64 GetMaxInt64(int pack) = 0;

	virtual RCBString GetMaxString(int pack) = 0;
	virtual RCBString GetMinString(int pack) = 0;
//	virtual RCDateTime GetMaxDateTime(int pack) const {BHASSERT(false, "not overridden"); return 0; };
//	virtual RCDateTime GetMinDateTime(int pack) const {BHASSERT(false, "not overridden"); return 0; };
//	virtual RCNum GetMaxNum(int pack) const {BHASSERT(false, "not overridden"); return 0.0; };
//	virtual RCNum GetMinNum(int pack) const {BHASSERT(false, "not overridden"); return 0.0; };

	/*!
	 * \brief Get number of NULLs in the datapack identified by \e pack
	 * Special case: GetNoNulls(-1) gives a total number of nulls.
	 * Value NULL_VALUE_64 means 'cannot determine'.
	 */
	virtual _int64 GetNoNulls(int pack) = 0;

	/*!
	 * \brief return true if sure that the column contains nulls only
	 */
	virtual bool RoughNullsOnly() const = 0;

	//! \brief Get the sum of values in the datapack identified by \e pack
	virtual _int64 GetSum(int pack, bool &nonnegative) = 0;

	virtual std::vector<_int64> GetListOfDistinctValuesInPack(int pack)
	{ std::vector<_int64> empty; return empty; }

	//! provide the most probable approximation of number of objects matching the condition
	virtual _uint64 ApproxAnswerSize(Descriptor& d) { BHASSERT(false, "not overridden"); return 0; }

	virtual _uint64 ApproxDistinctVals(bool incl_nulls, Filter* f, RSValue* rf, bool outer_nulls_possible) = 0;	// provide the best upper approximation of number of diff. values (incl. null)

	virtual _uint64 ExactDistinctVals(Filter* f) = 0;			// provide the exact number of diff. non-null values, if possible, or NULL_VALUE_64

	virtual ushort MaxStringSize(Filter* f = NULL) = 0;		// maximal byte string length in column

	/*! \brief Are all the values unique?
	*/
	bool IsUnique() const { return is_unique; }

	/*! \brief Sets the uniqueness status
	*
	* The status is changed in memory only
	* \param unique true if column has been verified to contain unique values
	*/
	void SetUnique(bool unique) { is_unique = unique; }

	/*! \brief Is Unique status updated (valid)?
	*
	*/
	bool IsUniqueUpdated() const { return is_unique_updated; }

	/*! \sa  IsUniqueUpdated
	*
	*/
	void SetUniqueUpdated(bool updated) { is_unique_updated = updated; }


	//! shortcut utility function = IsUniqueUpdated && IsUnique
	RSValue IsDistinct() const			{ return (IsUniqueUpdated()? (IsUnique()? RS_ALL : RS_NONE) : RS_UNKNOWN); } // three-valued answer

	virtual _int64 RoughMin(Filter* f = NULL, RSValue* rf = NULL) = 0;		// for numerical: best rough approximation of min for a given filter (or global min if filter is NULL)
	virtual _int64 RoughMax(Filter* f = NULL, RSValue* rf = NULL) = 0;		// for numerical: best rough approximation of max for a given filter (or global max if filter is NULL)
	virtual void GetTextStat(TextStat &s, Filter *f = NULL)
	{ s.Invalidate(); }
	virtual double RoughSelectivity()
	{ return 1; }

	/*! \brief Return true if the column (filtered) contain only non-null distinct values
	 */
	virtual bool IsDistinct(Filter* f) = 0;

	//! \brief Is the pack NULLS_ONLY, UNIFORM, NORMAL etc...
	virtual PackOntologicalStatus GetPackOntologicalStatus(int pack_no) = 0;

	/*! Check whether the value identified by \e row meets the condition \e d and store the result in the filter
	 * \param  row row number
	 * \param f affected Filter, associated with \e it
	 * \param d condition to be checked
	 *
	 * \pre requires a new Filter functionality:
	 * Tell, that changes to the current packrow are finished
	 * void Filter::SetDelayed();
	 * void Filter::CommitPack();
	 * Apply all pending changes to the Filter, so they become visible
	 * void Filter::Commit();
	 */
	//virtual void UpdateFilterDelayed(_uint64 row, Filter& f, Descriptor& d) = 0;

	/*! Check whether the value identified by \e row meets the condition \e d and store the result in the filter
	 * \param row row number
	 * \param f affected Filter, associated with \e it
	 * \param d condition to be checked
	 */
	//virtual void UpdateFilter(_uint64 row, Filter& f, Descriptor& d) = 0;

	/*! Check the condition \e desc on a packrow identified iterator by \e mit and update it respectively
	* \param mit - updating iterator representing a pack
	* \param dim - dimension in corresponding multiindex
	* \param desc - condition to be checked
	*/
	virtual void EvaluatePack(MIUpdatingIterator &mit, int dim, Descriptor& desc) = 0;

	/*! Check the condition \e d on a datarow from \e mind identified by \e pack
	 * \param row row number
	 * \param d condition to be checked
	 */
	//virtual bool CheckCondition(_uint64 row, Descriptor& d) = 0;

	//! check whether any value from the pack may meet the condition
	virtual RSValue RoughCheck(int pack, Descriptor& d, bool additional_nulls_possible) = 0;

	//! check whether any pair from two packs of two different attr/tables may meet the condition
	virtual RSValue RoughCheck(int pack1, int pack2, Descriptor& d) = 0;
	virtual RSValue RoughCheckBetween(int pack, _int64 min, _int64 max)			{ return RS_SOME; }

	virtual bool TryToMerge(Descriptor &d1,Descriptor &d2) = 0;

	virtual void DisplayAttrStats(Filter *f) = 0;
	virtual uint AttrNo() = 0;

	virtual ~PhysicalColumn();

	/*! \brief Get the name of the column (null terminated)
	 * \return A pointer to a static char buffer
	 */
	const char* Name() const {return name;};
	/*! \brief Set the name of the column (must be null terminated)
	 * \return A pointer to a static char buffer
	 */
	void  SetName(const char *a_name);

	/*! \brief Get the description of the column (null terminated)
	 * \return A pointer to a static char buffer
	 */
	const char* Description() const { return desc; }
	void  SetDescription(const char *a_desc);

	/*! \brief For lookup (RCAttr) columns only: decode a lookup value
	 * \return Text value encoded by a given code
	 */
	virtual RCBString	DecodeValue_S(_int64 code) = 0;
	virtual int			EncodeValue_S(RCBString &v) = 0;
	virtual enum phys_col_t ColType() = 0;

private:
	char* name;
	char* desc;

	bool is_unique;
	bool is_unique_updated;
};

#endif /* PHYSICALCOLUMN_H_ */
