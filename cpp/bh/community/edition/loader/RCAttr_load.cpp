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

#include "common/CommonDefinitions.h"
#include "core/RCAttrPack.h"
#include "core/tools.h"
#include "core/WinTools.h"
#include "RCAttr_load.h"
#include "loader/NewValueSet.h"

using namespace std;

RCAttrLoad::RCAttrLoad(int a_num,int t_num, string const& a_path,int conn_mode,unsigned int s_id, DTCollation collation) throw(DatabaseRCException)
	: RCAttrLoadBase(a_num, t_num, a_path, conn_mode, s_id, collation )
{
}

RCAttrLoad::~RCAttrLoad()
{
	LogWarnigs();
}

void RCAttrLoad::SavePacks()
{
	if(current_state!=1)
	{
		rclog << lock << "Error: cannot save. It is read only session." << unlock;
		throw InternalRCException("Can not save data. It is read only session.");
	}
	LoadPackInfo();
	int npack=NoPack();
	for(uint i=0;i<npack-packs_omitted;i++)
		SavePack(i);

}

int RCAttrLoad::Save()
{
	if(current_state!=1)
	{
		rclog << lock << "Error: cannot save. It is read only session." << unlock;
		throw InternalRCException("Can not save data. It is read only session.");
	}
	LoadPackInfo();

	if(rsi_hist_update || rsi_cmap_update)
		SaveRSI();
	BHASSERT(FileFormat()==10, "should be 'file_format==10'");
	SaveDPN();
	SaveHeader();
	return 0;
}

int RCAttrLoad::DoSavePack(int n, boost::shared_ptr<NewValuesSetBase>& to_release)	// WARNING: assuming that all packs with lower numbers are already saved!
{
	if(current_state != 1) {
		rclog << lock << "Error: cannot save. Read only session." << unlock;
		throw;
	}
	LoadPackInfo();

	if(!dpns[n].pack || dpns[n].pack->IsEmpty() || dpns[n].pack_mode == PACK_MODE_TRIVIAL || dpns[n].pack_mode == PACK_MODE_EMPTY) {
		if(PackType() == PackS && GetDomainInjectionManager().HasCurrent())
			AddOutliers(0);
		return 0;
	}

	if(dpns[n].pack->UpToDate())
		return 2;

	RCAttrLoad* rcattr = this;

	CompressionStatistics stats = rcattr->dpns[n].pack->Compress(rcattr->GetDomainInjectionManager());

	if(rcattr->PackType() == PackS && rcattr->GetDomainInjectionManager().HasCurrent())
		rcattr->AddOutliers(stats.new_no_outliers - stats.previous_no_outliers);

	//////////////// Update RSI ////////////////

	rcattr->UpdateRSI_Hist(n, rcattr->dpns[n].GetNoObj());		// updates pack n in RSIndices (NOTE: it may be executed in a random order of packs),
	rcattr->UpdateRSI_CMap(n, rcattr->dpns[n].GetNoObj());		// but does not save it to disk. Use SaveRSI to save changes (after the whole load)

	to_release.reset();

	bool last_pack = false;
	if (rcattr->dpns[n].pack_file != PF_NOT_KNOWN && rcattr->dpns[n].pack_file != PF_NULLS_ONLY && rcattr->dpns[n].pack_file != PF_NO_OBJ
			&& rcattr->dpns[n].pack_file>=0) {
		int pack_loc = rcattr->dpns[n].pack_file % 2;
		if (rcattr->dpns[n].pack_file==GetSaveFileLoc(pack_loc)
			|| (GetSavePosLoc(pack_loc)==0 && rcattr->dpns[n].pack_file==GetSaveFileLoc(pack_loc)-2)) {
			// check file size to see if writing the last datapack (overwrite it)
			// or not (leave it and append a new version, so the following datapacks are not overwritten)
			try {
				IBFile fattr;
				fattr.OpenCreate(AttrPackFileName(n));
				fattr.Seek(0, SEEK_END);
				uint fsize = fattr.Tell();
				if (fsize == rcattr->dpns[n].pack_addr + rcattr->dpns[n].pack->PreviousSaveSize())
					last_pack = true;
			} catch(DatabaseRCException& e)	{
				rclog << lock << "Error: cannot get filesize " << AttrPackFileName(n) << ". " << GetErrorMessage(errno) << unlock;
				throw;
			}
		}
	}
	if (last_pack) {
		if (rcattr->dpns[n].pack_file % 2 != GetCurSaveLocation()) {
			// last pack, but from the wrong file (switching session)
			SetSaveFileLoc(1 - GetCurSaveLocation(), rcattr->dpns[n].pack_file);
			SetSavePosLoc(1 - GetCurSaveLocation(), rcattr->dpns[n].pack_addr);
			rcattr->dpns[n].pack_file = GetSaveFileLoc(GetCurSaveLocation());
			rcattr->dpns[n].pack_addr = GetSavePosLoc(GetCurSaveLocation());
		}
	}
	else {
		// new package
		rcattr->dpns[n].pack_file=GetSaveFileLoc(GetCurSaveLocation());
		rcattr->dpns[n].pack_addr=GetSavePosLoc(GetCurSaveLocation());
	}
	////////////////////////////////////////////////////////
	// Now the package has its final location.
	if(rcattr->dpns[n].pack_file >= rcattr->GetTotalPackFile()) // find the largest used file number
		rcattr->SetTotalPackFile(rcattr->dpns[n].pack_file + 1);
	BHASSERT(rcattr->dpns[n].pack_file >= 0, "should be 'rcattr->dpns[n].pack_file >= 0'");
	uint pack_des = rcattr->dpns[n].pack_addr;
	if(rcattr->dpns[n].pack && !rcattr->dpns[n].pack->IsEmpty())
		rcattr->SetSavePosLoc(rcattr->GetCurSaveLocation(), rcattr->dpns[n].pack_addr
				+ rcattr->dpns[n].pack->TotalSaveSize());
	if(rcattr->GetSavePosLoc(rcattr->GetCurSaveLocation()) > rcattr->file_size_limit) // more than 1.5 GB - start the next package from the new file
	{
		int cur_save_loc = rcattr->GetCurSaveLocation();
		rcattr->SetSavePosLoc(cur_save_loc, 0);
		int save_location = rcattr->GetSaveFileLoc(cur_save_loc);
		rcattr->SetSaveFileLoc(cur_save_loc, save_location + 2);
	}

	////////////////////////////////////////////////////////
	string file_name;

	try
	{
		IBFile fattr;
		fattr.OpenCreate(file_name = rcattr->AttrPackFileName(n));
		fattr.Seek(pack_des, SEEK_SET);
		rcattr->dpns[n].pack->Save(&fattr, rcattr->GetDomainInjectionManager());
		fattr.Close();
	}
	catch(DatabaseRCException& e)
	{
		rclog << lock << "Error: can not save data pack to " << file_name << ". " << GetErrorMessage(errno) << unlock;
		throw;
	}
	rcattr->UnlockPackFromUse(n);
	return 0;
}

