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

#include <boost/bind.hpp>

#include "system/BHToolkit.h"
#include "edition/local.h"
#include "edition/loader/RCAttr_load.h"
#include "DataLoader.h"
#include "RCTable_load.h"
#include "system/Buffer.h"
#include "core/tools.h"

using namespace std;

RCTableLoad::RCTableLoad(string const& a_path, int current_state, vector<DTCollation> charsets) throw(DatabaseRCException)
	:	RCTableImpl(a_path, charsets, current_state, RCTableImpl::OpenMode::FOR_LOADER), no_loaded_rows(0), no_rejected_rows(0)
{
	if(no_attr > 0) {
		try {
			LoadAttribute(0);
		} catch(DatabaseRCException&) {
			throw;
		}

		m_update_time = a[0]->UpdateTime();
	} else 
		m_update_time = m_create_time;
}

void RCTableLoad::WaitForSaveThreads()
{
	for(int at = 0; at < no_attr; at++) {
		if(a[at])
			((RCAttrLoad*)a[at])->WaitForSaveThreads();
	}
}

void RCTableLoad::WaitForSaveThreadsNoThrow()
{
	for(int at = 0; at < no_attr; at++) {
		if(a[at])
			((RCAttrLoad*)a[at])->WaitForSaveThreadsNoThrow();
	}
}

void RCTableLoad::LoadData(IOParameters& iop)
{
	LargeBuffer rclb;

#ifndef __BH_COMMUNITY__ /* DATA PROCESSOR - <michal> to refactor !!!*/
	if(iop.GetEDF() == INFOBRIGHT_DF) {

		vector<RCAttrLoad*> attrs;
		for(uint i = 0; i < NoAttrs(); i++)	{
			LoadAttribute(i);
			((RCAttrLoad*)a[i])->LoadPackInfoForLoader();
			a[i]->GetDomainInjectionManager().SetTo(iop.GetDecompositions()[i]);
			attrs.push_back((RCAttrLoad*)a[i]);
		}
		LockPackInfoForUse();

		try {
			auto_ptr<DataLoader> loader = DataLoaderEnt::CreateDataLoader(attrs, rclb, iop);
			no_loaded_rows = loader->Proceed();
			//cerr << "loader->GetPackrowSize() = " << loader->GetPackrowSize() << endl;
			iop.SetPackrowSize(loader->GetPackrowSize());
			for(uint i = 0; i < NoAttrs(); i++)
				iop.SetNoOutliers(i, ((RCAttrLoadBase*)a[i])->GetNoOutliers());

		} catch(...) {
			WaitForSaveThreadsNoThrow();
			UnlockPackInfoFromUse();
			for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::DeleteRSIs, _1));
			throw;
		}

		WaitForSaveThreads();
		UnlockPackInfoFromUse();
		return;
	}
#endif

	if (!rclb.IsAllocated())
		throw OutOfMemoryRCException("Unable to create largebuffer due to insufficient memory.");

	vector<RCAttrLoad*> attrs;
	for(uint i = 0; i < NoAttrs(); i++)	{
		LoadAttribute(i);
		((RCAttrLoad*)a[i])->LoadPackInfoForLoader();
		a[i]->GetDomainInjectionManager().SetTo(iop.GetDecompositions()[i]);
		attrs.push_back((RCAttrLoad*)a[i]);
	}
	LockPackInfoForUse();
	try {
		rclb.BufOpen(iop, READ);
		if((rclb.BufStatus() != 1 && rclb.BufStatus() != 4 && rclb.BufStatus() != 5))
			throw FileRCException("Unable to open " + (IsPipe(iop.Path()) ? string("pipe ") : string("file ")) + string(iop.Path()) + string("."));
		if(rclb.BufSize()) {
			auto_ptr<DataLoader> loader = DataLoaderImpl::CreateDataLoader(attrs, rclb, iop);
			no_loaded_rows = loader->Proceed();
			no_rejected_rows = loader->GetNoRejectedRows();
		}
		rclb.BufClose();
		for(uint i = 0; i < NoAttrs(); i++)
			iop.SetNoOutliers(i, ((RCAttrLoadBase*)a[i])->GetNoOutliers());
	} catch(...) {
		WaitForSaveThreadsNoThrow();
		rclb.BufClose();
		UnlockPackInfoFromUse();
		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::DeleteRSIs, _1));
		throw;
	}
	WaitForSaveThreads();
	UnlockPackInfoFromUse();
}

void RCTableLoad::LoadData(IOParameters& iop, Buffer& buffer)
{
	vector<RCAttrLoad*> attrs;
	for(uint i = 0; i < NoAttrs(); i++)	{
		LoadAttribute(i);
		((RCAttrLoad*)a[i])->LoadPackInfoForLoader();
		a[i]->GetDomainInjectionManager().SetTo(iop.GetDecompositions()[i]);
		attrs.push_back((RCAttrLoad*)a[i]);
	}
	LockPackInfoForUse();
	try {
		auto_ptr<DataLoader> loader = DataLoader::CreateDataLoader(attrs, buffer, iop);
		loader->Proceed();
		for(uint i = 0; i < NoAttrs(); i++)
			iop.SetNoOutliers(i, ((RCAttrLoadBase*)a[i])->GetNoOutliers());
	} catch(...) {
		WaitForSaveThreadsNoThrow();
		UnlockPackInfoFromUse();
		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::DeleteRSIs, _1));
		throw;
	}
	WaitForSaveThreads();
	UnlockPackInfoFromUse();
}

RCTableLoad::~RCTableLoad()
{
	// Note:: there was not critical section destroy function in the windows version of this destructor. ???
	// we added the destroy in the linux port.
	//pthread_mutex_destroy(&synchr);
	WaitForSaveThreadsNoThrow();
}

inline void RCTableLoad::LoadAttribute(int attr_no)
{
	if(!a[attr_no])
	{
		if(!a[attr_no]) {
			try {
				a[attr_no] = new RCAttrLoad(attr_no, tab_num, path, conn_mode, session, charsets[attr_no]);
				if (a[attr_no]->OldFormat()) {
					a[attr_no]->UpgradeFormat();
					delete a[attr_no];
					a[attr_no] = new RCAttrLoad(attr_no, tab_num, path, conn_mode, session, charsets[attr_no]);
				}
			} catch(DatabaseRCException&) {
				throw;
			}
		}
	}
}

_int64 RCTableLoad::NoRecordsLoaded()
{
	return no_loaded_rows;
}
