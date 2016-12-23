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

#include "edition/local.h"
#include "Query.h"
#include "ParametrizedFilter.h"
#include "ValueSet.h"
#include "CompiledQuery.h"
#include "TempTable.h"
#include "MysqlExpression.h"
#include "vc/ExpressionColumn.h"
#include "vc/SingleColumn.h"
#include "vc/ConstColumn.h"
#include "vc/InSetColumn.h"
#include "vc/SubSelectColumn.h"
#include "edition/core/Transaction.h"

using namespace std;

#ifdef __GNUC__
#define _ASSERT(x) assert(x)
#endif

TempTable* Query::Preexecute(CompiledQuery& qu, ResultSender* sender, bool display_now)
{
	if(rccontrol.isOn() && display_now)	{
		rccontrol.lock(m_conn->GetThreadID());
		qu.Print(this);
		rccontrol << unlock;
	}
	bool force_mysql_execution = false;

	////////////// Set up the structures
	int no_conds; //no_virt_columns;
	int i;
   	no_conds = qu.NoConds(); // number of filters in CompiledQuery q
	Condition** conds = NULL;
	if(no_conds > 0)
		conds = new Condition * [no_conds];
	for(i = 0; i < no_conds; i++) {
		conds[i] = NULL;
	}
	int last_cond = 0;

	TempTable* output_table = NULL;			// NOTE: this pointer will be returned by the function

	ta.resize(qu.NoTabs());
	int no_ta = 0;
	CompiledQuery::CQStep step;
	///////////// Preprocessing: special commands
	for(i = 0; i < qu.NoSteps(); i++) {
		step = qu.Step(i);
		if(step.alias && strcmp(step.alias, "bh_force_mysql") == 0)	{ // magical word (passed as table alias) to force MySQL execution
			force_mysql_execution = true;
			break;
		}
	}

	std::pair<_int64, _int64> global_limits = qu.GetGlobalLimit();

#ifndef __BH_COMMUNITY__

	// Free unused delete masks
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	if (last_join && !(m_conn->Thd().spcont!=NULL && m_conn->Thd().spcont->is_function()))
		for (int j = 0; j < no_tab; j++)
			if (last_join[j]==-1)
				m_conn->GetTransaction()->PartialFree(t[j]->GetID());
#endif

#endif
	cq = &qu;
	///////////// Execution itself
	for(i = 0; i < qu.NoSteps(); i++) {
		// Local variables
		step = qu.Step(i);
		JustATablePtr t1_ptr, t2_ptr, t3_ptr;

		if(step.t1.n != NULL_VALUE_32) {
			if(step.t1.n>=0)
				t1_ptr=Table(step.t1.n)->shared_from_this();		// normal table
			else {
				t1_ptr=ta[-step.t1.n-1];	// TempTable
			}
		}
		if(step.t2.n != NULL_VALUE_32) {
			if(step.t2.n>=0)
				t2_ptr=Table(step.t2.n)->shared_from_this();		// normal table
			else {
				t2_ptr=ta[-step.t2.n-1];	// TempTable
			}
		}
		if(step.t3.n != NULL_VALUE_32) {
			if(step.t3.n>=0)
				t3_ptr=Table(step.t3.n)->shared_from_this();		// normal table
			else {
				t3_ptr=ta[-step.t3.n-1];	// TempTable
			}
		}
		//////////////////// Some technical information ///////////////////
		if(step.alias && strcmp(step.alias, "roughstats") == 0)	{
			// magical word (passed as table alias) to display statistics
			((TempTable*)ta[-step.t1.n-1].get())->DisplayRSI();
		}

		if(step.alias && strcmp(step.alias, "roughattrstats") == 0) {
			// magical word (passed as table alias) to display attr. statistics
			m_conn->SetDisplayAttrStats();
		}

		////////////////// Implementation of steps ////////////////////////
		try	{
			if(force_mysql_execution)
				throw NotImplementedRCException("MySQL execution forced by user.");

			switch(step.type) {
				case CompiledQuery::TABLE_ALIAS:
					ta[-step.t1.n-1] = t2_ptr;
					no_ta++;
					break;
				case CompiledQuery::TMP_TABLE:
					assert(step.t1.n < 0);
					ta[-step.t1.n - 1] =  step.n1 ?
							TempTable::Create(ta[-step.tables1[0].n - 1].get(), step.tables1[0].n, this, true) :
							TempTable::Create(ta[-step.tables1[0].n - 1].get(), step.tables1[0].n, this);
					((TempTable*)ta[-step.t1.n - 1].get())->ReserveVirtColumns(qu.NoVirtualColumns(step.t1));
					no_ta++;
					break;
				case CompiledQuery::CREATE_CONDS:
			 		assert(step.t1.n < 0);
					step.e1.vc = (step.e1.vc_id != NULL_VALUE_32) ? ((TempTable*)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e1.vc_id) : NULL;
					step.e2.vc = (step.e2.vc_id != NULL_VALUE_32) ? ((TempTable*)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e2.vc_id) : NULL;
					step.e3.vc = (step.e3.vc_id != NULL_VALUE_32) ? ((TempTable*)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e3.vc_id) : NULL;
					if(step.n1 != OR_SUBTREE ) {// on result = false
						conds[step.c1.n] = new Condition();
						if(step.c2.IsNull()) {
							conds[step.c1.n]->AddDescriptor(step.e1, step.op, step.e2, step.e3, (TempTable *)ta[-step.t1.n-1].get(), qu.GetNoDims(step.t1), (step.op == O_LIKE || step.op == O_NOT_LIKE) ? char(step.n2) : '\\');
						} else {
							BHASSERT_WITH_NO_PERFORMANCE_IMPACT(conds[step.c2.n]->IsType_Tree());
							conds[step.c1.n]->AddDescriptor(static_cast<SingleTreeCondition*>(conds[step.c2.n])->GetTree(), 
								(TempTable *)ta[-step.t1.n-1].get(), qu.GetNoDims(step.t1));
						}
					} else { // on result = true
						if(step.c2.IsNull())
							conds[step.c1.n] = new SingleTreeCondition(step.e1, step.op, step.e2, step.e3, (TempTable *)ta[-step.t1.n-1].get(), qu.GetNoDims(step.t1), char(step.n2));
						else {
							BHASSERT_WITH_NO_PERFORMANCE_IMPACT(conds[step.c2.n]->IsType_Tree());
							conds[step.c1.n] = new Condition();
							conds[step.c1.n]->AddDescriptor(((SingleTreeCondition*)conds[step.c2.n])->GetTree(), (TempTable *)ta[-step.t1.n-1].get(), qu.GetNoDims(step.t1));
						}
					}
					last_cond++;
					break;
				case CompiledQuery::AND_F:
				case CompiledQuery::OR_F:
					_ASSERT(conds);
					if(conds[step.c1.n]->IsType_Tree()) { // on result = false
						BHASSERT_WITH_NO_PERFORMANCE_IMPACT(conds[step.c2.n]->IsType_Tree());
						LogicalOperator lop = (step.type == CompiledQuery::AND_F ? O_AND : O_OR);
						static_cast<SingleTreeCondition*>(conds[step.c1.n])->AddTree(lop, 
							static_cast<SingleTreeCondition*>(conds[step.c2.n])->GetTree(), qu.GetNoDims(step.t1));
					} else {
						BHASSERT_WITH_NO_PERFORMANCE_IMPACT(conds[step.c2.n]->IsType_Tree());
						conds[step.c1.n]->AddDescriptor(static_cast<SingleTreeCondition*>(conds[step.c2.n])->GetTree(),
							(TempTable *)ta[-qu.GetTableOfCond(step.c1).n - 1].get(), qu.GetNoDims(qu.GetTableOfCond(step.c1)));
					} 
					break;
				case CompiledQuery::OR_DESC: 
				case CompiledQuery::AND_DESC: {
					LogicalOperator lop = (step.type == CompiledQuery::AND_DESC ? O_AND : O_OR);
					step.e1.vc = (step.e1.vc_id != NULL_VALUE_32) ? ((TempTable*)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e1.vc_id) : NULL;
					step.e2.vc = (step.e2.vc_id != NULL_VALUE_32) ? ((TempTable*)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e2.vc_id) : NULL;
					step.e3.vc = (step.e3.vc_id != NULL_VALUE_32) ? ((TempTable*)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e3.vc_id) : NULL;
					if(!conds[step.c1.n]->IsType_Tree()) { 
						BHASSERT_WITH_NO_PERFORMANCE_IMPACT(conds && conds[step.c1.n]);
						conds[step.c1.n]->AddDescriptor(step.e1, step.op, step.e2, step.e3, (TempTable *)ta[-step.t1.n-1].get(), qu.GetNoDims(step.t1), (step.op == O_LIKE || step.op == O_NOT_LIKE) ? char(step.n2) : '\\');
					} else  
						static_cast<SingleTreeCondition*>(conds[step.c1.n])->AddDescriptor(lop, step.e1, step.op, step.e2, step.e3, (TempTable *)ta[-step.t1.n-1].get(), qu.GetNoDims(step.t1), (step.op == O_LIKE || step.op == O_NOT_LIKE) ? char(step.n2) : '\\');
					break;
				}
				case CompiledQuery::T_MODE:
					assert(step.t1.n<0 && ta[-step.t1.n-1]->TableType() == TEMP_TABLE);
					((TempTable *)ta[-step.t1.n-1].get())->SetMode(step.tmpar, step.n1, step.n2);
					break;
				case CompiledQuery::JOIN_T:
					assert(step.t1.n<0 && ta[-step.t1.n-1]->TableType() == TEMP_TABLE);
					((TempTable*)ta[-step.t1.n-1].get())->JoinT(t2_ptr.get(), step.t2.n, step.jt);
					break;
				case CompiledQuery::ADD_CONDS: {
					assert(step.t1.n<0 && ta[-step.t1.n-1]->TableType() == TEMP_TABLE);
					if(step.c1.n == NULL_VALUE_32)
						break;
					if(step.n1 != HAVING_COND)
						conds[step.c1.n]->Simplify();
					((TempTable*)ta[-step.t1.n-1].get())->AddConds(conds[step.c1.n], (CondType)step.n1);
					break;
				}
				case CompiledQuery::LEFT_JOIN_ON: {
					assert(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TEMP_TABLE);
					if(step.c1.n == NULL_VALUE_32)
						break;
					((TempTable*)ta[-step.t1.n-1].get())->AddLeftConds(conds[step.c1.n], step.tables1, step.tables2);
					break;
				}
				case CompiledQuery::INNER_JOIN_ON: {
					assert(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TEMP_TABLE);
					if(step.c1.n == NULL_VALUE_32)
						break;
					((TempTable*)ta[-step.t1.n-1].get())->AddInnerConds(conds[step.c1.n], step.tables1);
					break;
				}
				case CompiledQuery::APPLY_CONDS: {
					_int64 cur_limit = -1;
					if(qu.FindDistinct(step.t1.n))
						((TempTable *)ta[-step.t1.n-1].get())->SetMode(TM_DISTINCT, 0, 0);
					if(qu.NoAggregationOrderingAndDistinct(step.t1.n))
						cur_limit = qu.FindLimit(step.t1.n);

					if(cur_limit != -1 && ((TempTable *)ta[-step.t1.n-1].get())->GetFilterP()->NoParameterizedDescs())
						cur_limit = -1;

					ParameterizedFilter* filter = ((TempTable *)ta[-step.t1.n-1].get())->GetFilterP();
					set<int> used_dims = qu.GetUsedDims(step.t1, ta);

					// no need any more to check WHERE for not used dims
					bool is_simple_filter = true; // qu.IsSimpleFilter(step.c1);
					if(used_dims.size() == 1 && used_dims.find(NULL_VALUE_32) != used_dims.end()) 
						is_simple_filter = false;
					for(int i = 0; i < filter->mind->NoDimensions(); i++) {
						if(used_dims.find(i) == used_dims.end() && is_simple_filter)
							filter->mind->ResetUsedInOutput(i);
						else
							filter->mind->SetUsedInOutput(i);
					}

					if(IsRoughQuery()) {
						((TempTable*)ta[-step.t1.n-1].get())->GetFilterP()->RoughUpdateParamFilter();
					} else
						((TempTable*)ta[-step.t1.n-1].get())->GetFilterP()->UpdateMultiIndex(qu.CountColumnOnly(step.t1), cur_limit);
					break;
				}
				case CompiledQuery::ADD_COLUMN: {
					assert(step.t1.n<0 && ta[-step.t1.n-1]->TableType() == TEMP_TABLE);
					CQTerm e(step.e1);
					if (e.vc_id != NULL_VALUE_32)
						e.vc = ((TempTable*)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.e1.vc_id); //vc must have been created
					step.a1.n = ((TempTable*)ta[-step.t1.n-1].get())->AddColumn(e, step.cop, step.alias, step.n1 ? true : false);
					break;
				}
				case CompiledQuery::CREATE_VC: {
					assert(step.t1.n < 0 && ta[-step.t1.n - 1]->TableType() == TEMP_TABLE);
					TempTable* t = (TempTable*)ta[-step.t1.n - 1].get();

					assert(t);
					if(step.mysql_expr.size() > 0) {
					// VirtualColumn for Expression
						assert(step.mysql_expr.size() == 1);
						MultiIndex *mind = (step.t2.n == step.t1.n) ? t->GetOutputMultiIndexP() : t->GetMultiIndexP();
						int c = ((TempTable*)ta[-step.t1.n - 1].get())->AddVirtColumn(CreateColumnFromExpression(step.mysql_expr, t, step.t1.n, mind), step.a1.n);
						assert(c == step.a1.n);
					} else if(step.virt_cols.size() > 0) {
					// VirtualColumn for IN
						ColumnType ct;
						if(step.a2.n != NULL_VALUE_32)
							ct = ((TempTable*)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.a2.n)->Type();
						vector<VirtualColumn*> vcs;
						for(uint i = 0; i < step.virt_cols.size(); i++)
							vcs.push_back(((TempTable*)ta[-step.t1.n - 1].get())->GetVirtualColumn(step.virt_cols[i]));
						int c = ((TempTable*)ta[-step.t1.n - 1].get())->AddVirtColumn( new InSetColumn(ct, t->GetMultiIndexP(), vcs), step.a1.n);
						assert(c == step.a1.n);
					} else if(step.a2.n != NULL_VALUE_32) {
					// VirtualColumn for PhysicalColumn
						JustATable* t_src = ta[-step.t2.n - 1].get();
						PhysicalColumn* phc;
						MultiIndex *mind = (step.t2.n == step.t1.n) ? t->GetOutputMultiIndexP() : t->GetMultiIndexP();
						int dim = (step.t2.n == step.t1.n) ? 0 : t->GetDimension(step.t2);
						phc = (PhysicalColumn* ) t_src->GetColumn(step.a2.n >= 0 ? step.a2.n : -step.a2.n - 1);
						int c = ((TempTable*)ta[-step.t1.n - 1].get())->AddVirtColumn(  new SingleColumn(phc, mind, step.t2.n, step.a2.n, ta[-step.t2.n - 1].get(), dim), step.a1.n);
						assert(c == step.a1.n);
					} else {
					// VirtualColumn for Subquery
						assert(ta[-step.t2.n - 1]->TableType() == TEMP_TABLE);
						int c = ((TempTable*)ta[-step.t1.n - 1].get())->AddVirtColumn(
									new SubSelectColumn(dynamic_cast<TempTable*>(ta[-step.t2.n - 1].get()),
										step.n1 == 1 ? t->GetOutputMultiIndexP() : t->GetMultiIndexP(),
										t, step.t1.n),
								step.a1.n);
						assert(c == step.a1.n);
					}
					break;
				}
				case CompiledQuery::ADD_ORDER: {
						assert(step.t1.n < 0 && ta[-step.t1.n-1]->TableType() == TEMP_TABLE && step.n1 >= 0 && step.n1 < 2);
						assert(step.a1.n >= 0 && step.a1.n < qu.NoVirtualColumns(step.t1));
						TempTable *loc_t = (TempTable *)ta[-step.t1.n - 1].get();
						loc_t->AddOrder(loc_t->GetVirtualColumn(step.a1.n), (int)step.n1); // step.n1 = 0 for asc, 1 for desc
						break;
					}
				case CompiledQuery::UNION:
					assert(step.t1.n < 0 && step.t2.n < 0 && step.t3.n < 0);
					assert(ta[-step.t2.n - 1]->TableType() == TEMP_TABLE && (step.t3.n == NULL_VALUE_32 || ta[-step.t3.n - 1]->TableType() == TEMP_TABLE));
					if(step.t1.n != step.t2.n)
						ta[-step.t1.n - 1] = TempTable::Create(*(TempTable *)ta[-step.t2.n - 1].get(), false);
					if(IsRoughQuery()) {
						if(step.t3.n == NULL_VALUE_32)
							((TempTable *)ta[-step.t1.n - 1].get())->RoughUnion(NULL, qu.IsResultTable(step.t1) ? sender : NULL);
						else
							((TempTable *)ta[-step.t1.n - 1].get())->RoughUnion((TempTable *)ta[-step.t3.n - 1].get(), qu.IsResultTable(step.t1) ? sender : NULL);
					} else if(qu.IsResultTable(step.t1) && !qu.IsOrderedBy(step.t1) && step.n1)
						((TempTable *)ta[-step.t1.n - 1].get())->Union((TempTable *)ta[-step.t3.n - 1].get(), (int)step.n1, sender, global_limits.first, global_limits.second);
					else {
						if(step.t3.n == NULL_VALUE_32)
							((TempTable *)ta[-step.t1.n - 1].get())->Union(NULL, (int)step.n1);
						else {
							((TempTable *)ta[-step.t1.n - 1].get())->Union((TempTable *)ta[-step.t3.n - 1].get(), (int)step.n1);
							ta[-step.t3.n - 1].reset();
						}
					}
					break;
				case CompiledQuery::RESULT:
					assert(step.t1.n<0 && -step.t1.n-1<ta.size() && ta[-step.t1.n-1]->TableType() == TEMP_TABLE);
					output_table=(TempTable *)ta[-step.t1.n-1].get();
					break;
				case CompiledQuery::STEP_ERROR:
					rccontrol.lock(m_conn->GetThreadID()) << "ERROR in step " << step.alias << unlock;
					break;
				default:
					rccontrol.lock(m_conn->GetThreadID()) << "ERROR: unsupported type of CQStep (" << step.type << ")" << unlock;
			}
#ifndef __BH_COMMUNITY__
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
			// Free unused delete masks
			if (last_join && !(m_conn->Thd().spcont!=NULL && m_conn->Thd().spcont->is_function()))
				for (int j = 0; j < no_tab; j++)
					if (last_join[j]==i)
						m_conn->GetTransaction()->PartialFree(t[j]->GetID());
#endif
#endif
		} catch(...) {
			for(i = 0; i < no_conds; i++)
				delete conds[i];
			delete [] conds;
			throw;
		}
	}

	///////////// Cleanup

	for(i = 0; i < no_conds; i++)
		delete conds[i];
	delete [] conds;

	// NOTE: output_table is sent out of this function and should be managed elsewhere.
	// before deleting all TempTables but output_table those have to be detected
	// there are used by output_table

	return output_table;
}
