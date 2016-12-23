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

#ifndef RCATTR_LOAD_H_
#define RCATTR_LOAD_H_

#include "loader/RCAttrLoadBase.h"

class RCAttrLoad : public RCAttrLoadBase
{
public:
	RCAttrLoad(int a_num,int t_num, std::string const& a_path,int conn_mode=0,unsigned int s_id=0, DTCollation collation = DTCollation()) throw(DatabaseRCException);
	~RCAttrLoad();
	void WaitForSaveThreads() {};
	void WaitForSaveThreadsNoThrow() {};
	void SavePacks();
	int Save();
	void SetLargeBuffer(LargeBuffer* lb) {};

private:
	virtual void DoWaitForSaveThreads(){}
	virtual int DoSavePack(int n, boost::shared_ptr<NewValuesSetBase>& to_release);		// save pack, determine and update its address in file
};

#endif /* RCATTR_LOAD_H_*/
