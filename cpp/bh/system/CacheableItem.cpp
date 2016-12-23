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

#include "CacheableItem.h"
#include "RCSystem.h"
#include "core/tools.h"
#include "system/fet.h"

using namespace std;
using namespace bh;

CacheableItem::CacheableItem(char const* owner_name, char const* object_id, int _default_block_size)
{
	default_block_size = _default_block_size;
	assert(owner_name != NULL);
	assert(object_id != NULL);
	// copy the temporary folder first
	filename = NULL;

	try
	{
		// read the configuration parameter
		string temp_filename = Configuration::GetProperty(Configuration::CacheFolder);
		filename_n_position = temp_filename.length();
		filename = new char [filename_n_position + 37];			// "...path.../XXXXXXnnnnnnAAAAAAAABBBBBBBB.bh_tmp"
		strcpy(filename, temp_filename.c_str());
		if(filename[filename_n_position - 1] != '/' && filename[filename_n_position - 1] != '\\') {
			filename[filename_n_position] = '/';
			filename_n_position++;
		}
		filename_n_position += 6;
	} catch(Configuration::Exception&) {
		// no parameter - leave filename=NULL
	}

	if(filename == NULL) {
		// if the temporary path is not set, use the current folder
		filename = new char [36];			// "XXXXXXnnnnnnAAAAAAAABBBBBBBB.bh_tmp"
		filename_n_position = 6;
	}
	max_file_id = 0;
	max_file_pos = 0;
	no_block = 0;
	cur_file_number = -1;
	// fill the file name
	int i = 0, j = 0;
	while(owner_name[j] != 0 && i < 6) 
		filename[filename_n_position - 6 + (i++)] = owner_name[j++];
	while(i < 3) 
		filename[filename_n_position - 6 + (i++)] = '_';
	j = 0;
	while(object_id[j] != 0 && i < 6)
		filename[filename_n_position - 6 + (i++)] = object_id[j++];
	while(i < 6) 
		filename[filename_n_position - 6 + (i++)] = '_';
	strcpy(filename + filename_n_position, "000000");
	char buf[30];
	unsigned int random_number = 0;
	random_number |= ((rand() % 1024) << 21);
	random_number |= ((rand() % 1024) << 11);
	random_number |= (rand() % 2048);
	sprintf(buf, "%X", random_number);
	strcpy(filename + filename_n_position + 6 + (8 - strlen(buf)), buf);
	if(strlen(buf) < 8) 
		memset(filename + filename_n_position + 6, '0', 8 - strlen(buf));
	sprintf(buf, "%p", this);
	strcpy(filename + filename_n_position + 14 + (8 - strlen(buf)), buf);
	if(strlen(buf) < 8) 
		memset(filename + filename_n_position + 14, '0', 8 - strlen(buf));
	strcpy(filename + filename_n_position + 22, ".bh_tmp");
}

CacheableItem::~CacheableItem()
{
	cur_file_handle.Close();
	for(int i = 0; i <= max_file_id; i++) {
		SetFilename(i);			// delete all files
		RemoveFile(filename);
	}
	delete [] filename;
}

void CacheableItem::CI_Put(int block, unsigned char* data, int size)
{
	if(block == -1)
		return;
	if(size == -1) 
		size=default_block_size;
	if(size <= 0) 
		return;
	for(int i = no_block; i < block; i++)	{	// rare case: the block numbering is not continuous
		// create empty blocks
		file_number.push_back(-1);
		file_size.push_back(0);
		file_start.push_back(0);
	}
//	cout << "CI_Put[" << (_int64(this))%101 << "] : " << size  << ", file_size: "
//		 << (block<no_block ? file_size[block] : -1) << ", block: " << block << endl;
	try {
		if(block >= no_block || size != file_size[block]) {
			// create a new block or reallocate the existing one
			if((long long)(size) + (long long)(max_file_pos) > 2000000000) {		// the file size limit: 2 GB
				// file becomes too large, start the next one!
				max_file_id++;
				max_file_pos = 0;
			}
			if(block >= no_block) {
				file_number.push_back(max_file_id);
				file_size.push_back(size);
				file_start.push_back(max_file_pos);
				no_block = block + 1;
			} else {
				file_number[block] = max_file_id;
				file_size[block] = size;
				file_start[block] = max_file_pos;
			}
			cur_file_handle.Close();
			SetFilename(file_number[block]);
			if(max_file_pos == 0)				// the new file
				cur_file_handle.OpenCreateEmpty(filename);
			else
				cur_file_handle.OpenReadWrite(filename);
			assert(cur_file_handle.IsOpen());
			cur_file_number = file_number[block];
			max_file_pos += size;
		}
		// save the block
		if(file_number[block] != cur_file_number) {
			// open the block file
			cur_file_number = file_number[block];
			cur_file_handle.Close();
			SetFilename(cur_file_number);
			cur_file_handle.OpenReadWrite(filename);
			assert(cur_file_handle.IsOpen());
		}

#ifdef FUNCTIONS_EXECUTION_TIMES
		char str[100];
		if(file_size[block] >= 1024*1024)
			sprintf(str, "CacheableItem::CI_Put,write(%dMB)", (int)(file_size[block]/1024/1024));
		else
			sprintf(str, "CacheableItem::CI_Put,write(%dKB)", (int)(file_size[block]/1024));
		FETOperator feto(str);
#endif
		cur_file_handle.Seek(file_start[block], SEEK_SET);
		cur_file_handle.WriteExact((char*)data, file_size[block]);
	} catch(DatabaseRCException& e)	{
		throw OutOfMemoryRCException(e.what());
	}
}

int CacheableItem::CI_Get(int block, uchar* data, int size, int off)
{
	if(block >= no_block || file_size[block] <= 0 || (size >= 0 && off + size > file_size[block])) 
		return -1;

	try {
		// open file containing the block
		if(file_number[block] != cur_file_number) {
			// open the block file
			cur_file_number = file_number[block];
			cur_file_handle.Close();
			SetFilename(cur_file_number);
			cur_file_handle.OpenReadWrite(filename);
			assert(cur_file_handle.IsOpen());
		}

		// load the block
		if(size < 0) {
			size = file_size[block];
			off = 0;
		}

#ifdef FUNCTIONS_EXECUTION_TIMES
		char str[100];
		if(size >= 1024*1024)
			sprintf(str, "CacheableItem::CI_Get,read(%dMB)", (int)(size/1024/1024));
		else
			sprintf(str, "CacheableItem::CI_Get,read(%dKB)", (int)(size/1024));
		FETOperator feto(str);
#endif

		cur_file_handle.Seek(file_start[block] + off, SEEK_SET);
		cur_file_handle.Read((char*)data, size);
	} catch(DatabaseRCException& e)	{
		throw OutOfMemoryRCException(e.what());
	}
	return 0;
}

void CacheableItem::SetFilename(int i) // char -> void temporary change to enable compilation
{
	filename[filename_n_position+5] = (char)('0'+i%10);
	filename[filename_n_position+4] = (char)('0'+(i/10)%10);
	filename[filename_n_position+3] = (char)('0'+(i/100)%10);
	filename[filename_n_position+2] = (char)('0'+(i/1000)%10);
	filename[filename_n_position+1] = (char)('0'+(i/10000)%10);
	filename[filename_n_position] = (char)('0'+(i/100000)%10);
}


