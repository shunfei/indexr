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

#include "core/RCEngine.h"
#include "common/bhassert.h"

int RCEngine::Convert(int& is_null, Field* field, void* out_ptr, RCDataType& rcitem, std::vector<uchar>* blob_buf)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(rcitem.IsNull()) {
		is_null = 1;
		return 1;
	}
	is_null = 0;
	bool done = true;
	switch(field->type()) {
		case MYSQL_TYPE_VARCHAR: {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<RCBString*>(&rcitem));
			RCBString& str_val = (RCBString&)rcitem;
			if(str_val.size() > field->field_length)
				throw DatabaseRCException("Incorrect field size");
			if(field->field_length <= 255)
				str_val.PutVarchar((char*&)out_ptr, 1, false);
			else if(field->field_length <= ((1 << 16) - 1))
				str_val.PutVarchar((char*&)out_ptr, 2, false);
			break;
								 }
		case MYSQL_TYPE_STRING:
			if(dynamic_cast<RCBString*>(&rcitem)) {
				((RCBString&)rcitem).PutString((char*&)out_ptr, (ushort)field->field_length, false);
			} else {
				rcitem.ToRCString().PutString((char*&)out_ptr, (ushort)field->field_length, false);
			}
			break;
		case MYSQL_TYPE_BLOB: {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<RCBString*>(&rcitem));
			Field_blob *blob= (Field_blob*) field;
			if (blob_buf==NULL) {
				blob->set_ptr(((RCBString&)rcitem).len, (uchar*)((RCBString&)rcitem).val);
				blob->copy();
			} else {
				blob->store(((RCBString&)rcitem).val, ((RCBString&)rcitem).len, &my_charset_bin);
				uchar *src, *tgt;

				uint packlength= blob->pack_length_no_ptr();
				uint length= blob->get_length(blob->ptr);
				memcpy_fixed(&src, blob->ptr + packlength, sizeof(char*));
				if (src)
				{
					blob_buf->resize(length);
					tgt= &((*blob_buf)[0]);
					bmove(tgt, src, length);
					memcpy_fixed(blob->ptr + packlength, &tgt, sizeof(char*));
				}
			}
			break;
							  }
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_NEWDECIMAL: {
			my_decimal md;
			if(rcitem.Type() == RC_REAL) {
				double2decimal((double)((RCNum&)(rcitem)), &md);				
			} else
				RCEngine::Convert(is_null, &md, rcitem);			
			decimal_round(&md, &md, ((Field_new_decimal*)field)->decimals(), HALF_UP);
			decimal2bin(&md, (uchar*)out_ptr, ((Field_new_decimal*)field)->precision, ((Field_new_decimal*)field)->decimals());
			break;
									}
		default:
			done = false;
			break;
	}
	if(!done) {
		is_null = 0;
		switch(rcitem.Type()) {
			case RC_BYTEINT:
			case RC_SMALLINT:
			case RC_MEDIUMINT:
			case RC_INT:
			case RC_BIGINT:
			case RC_REAL:
			case RC_FLOAT:
			case RC_NUM:
				switch(field->type()) {
			case MYSQL_TYPE_TINY:
				*(char*)out_ptr =	(char)(_int64)((RCNum&)(rcitem));
				break;
			case MYSQL_TYPE_SHORT:
				*(short*)out_ptr =	(short)(_int64)((RCNum&)(rcitem));
				break;
			case MYSQL_TYPE_INT24:
				int3store((char*)out_ptr, (int)(_int64)((RCNum&)(rcitem)));
				break;
			case MYSQL_TYPE_LONG:
				*(int*)out_ptr =	(int)(_int64)((RCNum&)(rcitem));
				break;
			case MYSQL_TYPE_LONGLONG:
				*(_int64*)out_ptr =	(_int64)((RCNum&)(rcitem));
				break;
			case MYSQL_TYPE_FLOAT:
				*(float*)out_ptr =	(float)((RCNum&)(rcitem));
				break;
			case MYSQL_TYPE_DOUBLE:
				*(double*)out_ptr = (double)((RCNum&)(rcitem));
				break;
			default:
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"No data types conversion available!");
				break;
				}
				break;
			case RC_STRING:
				switch(field->type()) {
			case MYSQL_TYPE_VARCHAR: {
				RCBString& str_val = (RCBString&)rcitem;
				if(str_val.size() > field->field_length)
					throw DatabaseRCException("Incorrect field size");
				if(field->field_length <= 255) {
					str_val.PutVarchar((char*&)out_ptr, 1, false);
				} else if(field->field_length <= ((1 << 16) - 1)) {
					str_val.PutVarchar((char*&)out_ptr, 2, false);
				}
				break;
									 }
			case MYSQL_TYPE_STRING:
				((RCBString&)rcitem).PutString((char*&)out_ptr, (ushort)field->field_length, false);
				break;
			case MYSQL_TYPE_BLOB: {
				Field_blob *blob= (Field_blob*) field;
				if (blob_buf==NULL) {
					blob->set_ptr(((RCBString&)rcitem).len, (uchar*)((RCBString&)rcitem).val);
					blob->copy();
				} else {
					blob->store(((RCBString&)rcitem).val, ((RCBString&)rcitem).len, &my_charset_bin);
					uchar *src, *tgt;

					uint packlength= blob->pack_length_no_ptr();
					uint length= blob->get_length(blob->ptr);
					memcpy_fixed(&src, blob->ptr + packlength, sizeof(char*));
					if (src)
					{
						blob_buf->resize(length);
						tgt= &((*blob_buf)[0]);
						bmove(tgt, src, length);
						memcpy_fixed(blob->ptr + packlength, &tgt, sizeof(char*));
					}
				}
				break;
								  }
			case MYSQL_TYPE_DATE: {
				char tmp[10];
				char* tmpptr = tmp;
				((RCBString&)rcitem).PutString(tmpptr, ushort(sizeof(tmp)), false);
				((Field_date*)field)->store(tmp, sizeof(tmp),NULL);
				break;
								  }
			case MYSQL_TYPE_TIME: {
				char tmp[10];
				char* tmpptr = tmp;
				((RCBString&)rcitem).PutString(tmpptr, ushort(sizeof(tmp)), false);
				((Field_time*)field)->store(tmp,sizeof(tmp),NULL);
				break;
								  }
			case MYSQL_TYPE_DATETIME: {
				char tmp[19];
				char* tmpptr = tmp;
				((RCBString&)rcitem).PutString(tmpptr, ushort(sizeof(tmp)), false);
				((Field_datetime*)field)->store(tmp,sizeof(tmp),NULL);
				break;
									  }
			default:
				((RCBString&)rcitem).PutString((char*&)out_ptr, (ushort)field->field_length, false);
				break;
				}

				break;
			case RC_YEAR: {
				if(field->type() == MYSQL_TYPE_YEAR) {
					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
						int y = rcdt->Year();
						*(char*)out_ptr = (y != 0 ? y - 1900 : y);
						if(field->field_length != 2)
							field->field_length = 4;
					} else
						assert(0);
				} else
					assert(0);
				break;
						  }
			case RC_DATE: {
				if(field->type() == MYSQL_TYPE_DATE || field->type() == MYSQL_TYPE_NEWDATE) {
					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
						uint n_val = 0;
						if(field->real_type() != MYSQL_TYPE_NEWDATE) {
							n_val += rcdt->Year();
							n_val *= 100;
							n_val += rcdt->Month();
							n_val *= 100;
							n_val += rcdt->Day();
							*(int*)out_ptr = n_val;
						} else {
							n_val |= (rcdt->Day()); //day
							n_val |= (rcdt->Month()) << 5; //mth
							n_val |= (rcdt->Year()) << 9; //year
							int3store((char*)out_ptr, n_val);
						}
					} else
						assert(0);
				}
				break;
						  }
			case RC_TIME: {
				if(field->type() == MYSQL_TYPE_TIME) {
					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
						long n_val = rcdt->Hour();
						n_val *= 100;
						n_val += rcdt->Minute();
						n_val *= 100;
						n_val += rcdt->Second();
						if(rcdt->IsNegative())
							n_val = -n_val;
						int3store((char*)out_ptr, n_val);
					} else
						assert(0);
				}
				break;
						  }
			case RC_DATETIME: {
				if(field->type() == MYSQL_TYPE_DATETIME) {
					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
						ulonglong n_val = 0;
						n_val += rcdt->Year();
						n_val *= 100;
						n_val += rcdt->Month();
						n_val *= 100;
						n_val += rcdt->Day();
						n_val *= 100;
						n_val += rcdt->Hour();
						n_val *= 100;
						n_val += rcdt->Minute();
						n_val *= 100;
						n_val += rcdt->Second();

						*(longlong*)out_ptr = n_val;
					}
				}
				break;
							  }
			case RC_TIMESTAMP: {
				if(field->type() == MYSQL_TYPE_TIMESTAMP) {
					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
						if(*rcdt != RC_TIMESTAMP_SPEC) {
							MYSQL_TIME myt;
							myt.year = rcdt->Year();
							myt.month = rcdt->Month();
							myt.day = rcdt->Day();
							myt.hour = rcdt->Hour();
							myt.minute = rcdt->Minute();
							myt.second = rcdt->Second();
							myt.time_type = MYSQL_TIMESTAMP_DATETIME;
							my_time_t secs = sec_since_epoch_TIME(&myt);
							//my_time_t mytt2 = my_system_gmt_sec(&myt, &mytz, &myb);
							*(int*)out_ptr = (int)(long)secs;
						} else {
							*(int*)out_ptr = 0;
						}
					}
				} else if(field->type() == MYSQL_TYPE_DATETIME) {
					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
						ulonglong n_val = 0;
						n_val += rcdt->Year();
						n_val *= 100;
						n_val += rcdt->Month();
						n_val *= 100;
						n_val += rcdt->Day();
						n_val *= 100;
						n_val += rcdt->Hour();
						n_val *= 100;
						n_val += rcdt->Minute();
						n_val *= 100;
						n_val += rcdt->Second();

						*(longlong*)out_ptr = n_val;
					}
				}
				break;
							   }
			default:
				return 0;
		}
	}
	return 1;
#endif
}

////int RCEngine::Convert(int& is_null, Field* field, void* out_ptr, RCDataType& rcitem, std::vector<uchar>* blob_buf)
////{
////#ifdef PURE_LIBRARY
////	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
////	return 0;
////#else
////<<<<<<< .working
////=======
////	if(rcitem->IsNull()) {
////		is_null = 1;
////		return 1;
////	}
////	is_null = 0;
////	bool done = true;
////	switch(field->type()) {
////		case MYSQL_TYPE_VARCHAR: {
////			RCBString str_val = (rcitem->Value()).ToRCString();
////			if(str_val.size() > field->field_length)
////				throw DatabaseRCException("Incorrect field size");
////			if(field->field_length <= 255)
////				str_val.PutVarchar((char*&)out_ptr, 1, false);
////			else if(field->field_length <= ((1 << 16) - 1))
////				str_val.PutVarchar((char*&)out_ptr, 2, false);
////			break;
////		}
////		case MYSQL_TYPE_STRING:
////			(rcitem->Value().Get())->ToRCString().PutString((char*&)out_ptr, (ushort)field->field_length, false);
////			break;
////		case MYSQL_TYPE_BLOB: {
////			RCBString str = (rcitem->Value()).ToRCString();
////			((Field_blob*)field)->set_ptr(str.len, (uchar*)str.val);
////			((Field_blob*)field)->copy();
////			break;
////		}
////		case MYSQL_TYPE_DECIMAL:
////		case MYSQL_TYPE_NEWDECIMAL: {
////			my_decimal md;
////			if(rcitem->Type() == RC_REAL)
////				double2decimal(*(double*)rcitem->Buf(), &md);
////			else 
////				RCEngine::Convert(is_null, &md, rcitem);
////			decimal_round(&md, &md, ((Field_new_decimal*)field)->decimals(), HALF_UP);
////			decimal2bin(&md, (uchar*)out_ptr, ((Field_new_decimal*)field)->precision, ((Field_new_decimal*)field)->decimals());
////			break;
////		}
////		default:
////			done = false;
////			break;
////	}
////	if(!done) {
////		//if(!RCEngine::AreConvertible(rcitem, field)) {
////		//	BHERROR("BH result type not convertible to MySQL type");
////		//	return 0;
////		//}
////		is_null = 0;
////		switch(rcitem->Type()) {
////			case RC_BYTEINT:
////				switch(field->type()) {
////					case MYSQL_TYPE_LONGLONG:
////						*(_int64*)out_ptr = *(char*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_LONG:
////						*(int*)out_ptr = *(char*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_SHORT:
////						*(short*)out_ptr = (short)*(char*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_FLOAT:
////						*(float*)out_ptr = (float)*(char*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_DOUBLE:
////						*(double*)out_ptr = (double)*(char*)rcitem->Buf();
////						break;
////					default:
////						*(char*)out_ptr = *(char*)rcitem->Buf();
////						break;
////				}
////				break;
////			case RC_SMALLINT:
////				switch(field->type()) {
////					case MYSQL_TYPE_LONGLONG:
////						*(_int64*)out_ptr = *(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_LONG:
////						*(int*)out_ptr = *(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_SHORT:
////						*(short*)out_ptr = (short)*(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_FLOAT:
////						*(float*)out_ptr = (float)*(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_DOUBLE:
////						*(double*)out_ptr = (double)*(int*)rcitem->Buf();
////						break;
////					default:
////						*(short*)out_ptr = *(short*)rcitem->Buf();
////						break;
////				}
////				break;
////			case RC_INT:
////				switch(field->type()) {
////					case MYSQL_TYPE_LONGLONG:
////						*(_int64*)out_ptr = *(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_LONG:
////						*(int*)out_ptr = *(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_SHORT:
////						*(short*)out_ptr = (short)*(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_FLOAT:
////						*(float*)out_ptr = (float)*(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_DOUBLE:
////						*(double*)out_ptr = (double)*(int*)rcitem->Buf();
////						break;
////					//case MYSQL_TYPE_NEWDECIMAL: {
////					//	my_decimal md;
////					//	RCEngine::Convert(is_null, &md, rcitem);
////					//	if(!is_null)
////					//		decimal2bin(&md, (uchar*)out_ptr, ((Field_new_decimal*)field)->precision, ((Field_new_decimal*)field)->decimals());
////					//	break;
////					//}
////  				    default: {
////					    *(int *)out_ptr = *static_cast<const int *>(rcitem->Buf());
////					    break;
////					}
////				}
////				break;
////			case RC_MEDIUMINT:
////				switch(field->type()) {
////					case MYSQL_TYPE_LONGLONG:
////						*(_int64*)out_ptr = *(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_LONG:
////						*(int*)out_ptr = *(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_SHORT:
////						*(short*)out_ptr = (short)*(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_FLOAT:
////						*(float*)out_ptr = (float)*(int*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_DOUBLE:
////						*(double*)out_ptr = (double)*(int*)rcitem->Buf();
////						break;
////					//case MYSQL_TYPE_NEWDECIMAL: {
////					//	my_decimal md;
////					//	RCEngine::Convert(is_null, &md, rcitem);
////					//	if(!is_null)
////					//		decimal2bin(&md, (uchar*)out_ptr, ((Field_new_decimal*)field)->precision, ((Field_new_decimal*)field)->decimals());
////					//	break;
////					//}
////					default:
////						int3store(static_cast<char*>(out_ptr), *static_cast<const int*>(rcitem->Buf()));
////						break;
////				}
////				break;
////			case RC_BIGINT:
////				switch(field->type()) {
////					case MYSQL_TYPE_LONG:
////						*(int*)out_ptr = (int)*(_int64*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_SHORT:
////						*(short*)out_ptr = (short)*(_int64*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_FLOAT:
////						*(float*)out_ptr = (float)*(_int64*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_DOUBLE:
////						*(double*)out_ptr = (double)*(_int64*)rcitem->Buf();
////						break;
////					//case MYSQL_TYPE_NEWDECIMAL: {
////					//	my_decimal md;
////					//	RCEngine::Convert(is_null, &md, rcitem);
////					//	if(!is_null)
////					//		decimal2bin(&md, (uchar*)out_ptr, ((Field_new_decimal*)field)->precision, ((Field_new_decimal*)field)->decimals());
////					//	break;
////					//}
////					default:
////						*(_int64*)out_ptr = *(_int64*)rcitem->Buf();
////						break;
////				}
////				break;
////			case RC_REAL:
////				switch(field->type()) {
////					//case MYSQL_TYPE_NEWDECIMAL: {
////					//	my_decimal md;
////					//	double2decimal(*(double*)rcitem->Buf(), &md);
////					//	decimal_round(&md, &md, ((Field_new_decimal*)field)->decimals(), HALF_UP);
////					//	decimal2bin(&md, (uchar*)out_ptr, ((Field_new_decimal*)field)->precision, ((Field_new_decimal*)field)->decimals());
////					//	break;
////					//}
////					case MYSQL_TYPE_FLOAT:
////						*(float*)out_ptr = (float)*(double*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_LONG:
////						*(int*)out_ptr = (int)*(double*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_SHORT:
////						*(short*)out_ptr = (short)*(double*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_LONGLONG:
////					case MYSQL_TYPE_INT24: 
////						*(_int64*)out_ptr = (_int64)*(double*)rcitem->Buf();
////						break;
////					default:
////						*(double*)out_ptr = *(double*)rcitem->Buf();
////						break;
////				}
////				break;
////			case RC_FLOAT:
////				switch(field->type()) {
////					case MYSQL_TYPE_LONGLONG:
////						*(_int64*)out_ptr = (_int64)*(double*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_LONG:
////						*(int*)out_ptr = (int)*(double*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_SHORT:
////						*(short*)out_ptr = (short)*(double*)rcitem->Buf();
////						break;
////					default:
////						*(float*)out_ptr = (float)*(double*)rcitem->Buf();
////						break;
////				}
////				break;
////			case RC_NUM: {
////				RCNum rcn = (RCNum)rcitem->Value();
////				switch(field->type()) {
////					case MYSQL_TYPE_TINY:
////						*(int*)out_ptr = (char)*(_int64*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_SHORT:
////						*(int*)out_ptr = (short)*(_int64*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_LONG:
////						*(int*)out_ptr = (int)*(_int64*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_LONGLONG:
////						*(_int64*)out_ptr = *(_int64*)rcitem->Buf();
////						break;
////					case MYSQL_TYPE_FLOAT:
////						*(float*)out_ptr = (float)rcn;
////						break;
////					case MYSQL_TYPE_DOUBLE:
////						*(double*)out_ptr = (double)rcn;
////						break;
////					default:
////						*(_int64*)out_ptr = (_int64)rcn;
////				}
////				break;
////			}
////			case RC_STRING:
////				switch(field->type()) {
////					case MYSQL_TYPE_VARCHAR: {
////						RCBString str_val = (rcitem->Value()).ToRCString();
////						if(str_val.size() > field->field_length)
////							throw DatabaseRCException("Incorrect field size");
////						if(field->field_length <= 255) {
////							//	*(uchar*)out_ptr = (uchar)(((RCDataType*)rcitem->Value())->ToRCString().len);
////							//	strcpy(((char*)out_ptr + 1), ((RCDataType*)rcitem->Value())->ToRCString());
////							str_val.PutVarchar((char*&)out_ptr, 1, false);
////						} else if(field->field_length <= ((1 << 16) - 1)) {
////							//	*(ushort*)out_ptr = (ushort)(((RCDataType*)rcitem->Value())->ToRCString().len);
////							//	strcpy(((char*)out_ptr + 2), ((RCDataType*)rcitem->Value())->ToRCString());
////							str_val.PutVarchar((char*&)out_ptr, 2, false);
////						}
////						break;
////					}
////					case MYSQL_TYPE_STRING:
////						(rcitem->Value().Get())->ToRCString().PutString((char*&)out_ptr, (ushort)field->field_length, false);
////						break;
////					case MYSQL_TYPE_BLOB: {
////						RCBString str = (rcitem->Value()).ToRCString();
////						((Field_blob*)field)->set_ptr(str.len, (uchar*)str.val);
////						((Field_blob*)field)->copy();
////						break;
////					}
////					case MYSQL_TYPE_DATE: {
////						char tmp[10];
////						char* tmpptr = tmp;
////						(rcitem->Value().Get())->ToRCString().PutString(tmpptr, ushort(sizeof(tmp)), false);
////						((Field_date*)field)->store(tmp,sizeof(tmp),NULL);
////						break;
////					}
////					case MYSQL_TYPE_TIME: {
////						char tmp[10];
////						char* tmpptr = tmp;
////						(rcitem->Value().Get())->ToRCString().PutString(tmpptr, ushort(sizeof(tmp)), false);
////						((Field_time*)field)->store(tmp,sizeof(tmp),NULL);
////						break;
////					}
////					case MYSQL_TYPE_DATETIME: {
////						char tmp[19];
////						char* tmpptr = tmp;
////						(rcitem->Value().Get())->ToRCString().PutString(tmpptr, ushort(sizeof(tmp)), false);
////						((Field_datetime*)field)->store(tmp,sizeof(tmp),NULL);
////						break;
////					}
////					default:
////						(rcitem->Value().Get())->ToRCString().PutString((char*&)out_ptr, (ushort)field->field_length, false);
////						break;
////				}
////
////				break;
////			case RC_YEAR: {
////				if(field->type() == MYSQL_TYPE_YEAR) {
////					//value = *(int*)rcitem->Buf();
////					RCValueObject rcvo = rcitem->Value();
////					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(rcvo.Get())) {
////						int y = rcdt->Year();
////						*(char*)out_ptr = (y != 0 ? y - 1900 : y);
////						if(field->field_length != 2)
////							field->field_length = 4;
////					} else
////						assert(0);
////				} else
////					assert(0);
////				break;
////			}
////			case RC_DATE: {
////				if(field->type() == MYSQL_TYPE_DATE || field->type() == MYSQL_TYPE_NEWDATE) {
////					//value = *(int*)rcitem->Buf();
////					RCValueObject rcvo = rcitem->Value();
////					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(rcvo.Get())) {
////						uint n_val = 0;
////						if(field->real_type() != MYSQL_TYPE_NEWDATE) {
////							n_val += rcdt->Year();
////							n_val *= 100;
////							n_val += rcdt->Month();
////							n_val *= 100;
////							n_val += rcdt->Day();
////							*(int*)out_ptr = n_val;
////						} else {
////							n_val |= (rcdt->Day()); //day
////							n_val |= (rcdt->Month()) << 5; //mth
////							n_val |= (rcdt->Year()) << 9; //year
////							int3store((char*)out_ptr, n_val);
////						}
////					} else
////						assert(0);
////				}
////				break;
////			}
////			case RC_TIME: {
////				if(field->type() == MYSQL_TYPE_TIME) {
////					RCValueObject rcvo = rcitem->Value();
////					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(rcvo.Get())) {
////						long n_val = rcdt->Hour();
////						n_val *= 100;
////						n_val += rcdt->Minute();
////						n_val *= 100;
////						n_val += rcdt->Second();
////						if(rcdt->IsNegative())
////							n_val = -n_val;
////						int3store((char*)out_ptr, n_val);
////					} else
////						assert(0);
////				}
////				break;
////			}
////			case RC_DATETIME: {
////				if(field->type() == MYSQL_TYPE_DATETIME) {
////					RCValueObject rcvo = rcitem->Value();
////					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(rcvo.Get())) {
////						ulonglong n_val = 0;
////						n_val += rcdt->Year();
////						n_val *= 100;
////						n_val += rcdt->Month();
////						n_val *= 100;
////						n_val += rcdt->Day();
////						n_val *= 100;
////						n_val += rcdt->Hour();
////						n_val *= 100;
////						n_val += rcdt->Minute();
////						n_val *= 100;
////						n_val += rcdt->Second();
////
////						*(longlong*)out_ptr = n_val;
////					}
////				}
////				break;
////			}
////			case RC_TIMESTAMP: {
////				if(field->type() == MYSQL_TYPE_TIMESTAMP) {
////					RCValueObject rcvo = rcitem->Value();
////					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(rcvo.Get())) {
////						if(*rcdt != RC_TIMESTAMP_SPEC) {
////							MYSQL_TIME myt;
////							myt.year = rcdt->Year();
////							myt.month = rcdt->Month();
////							myt.day = rcdt->Day();
////							myt.hour = rcdt->Hour();
////							myt.minute = rcdt->Minute();
////							myt.second = rcdt->Second();
////							myt.time_type = MYSQL_TIMESTAMP_DATETIME;
////							my_time_t secs = sec_since_epoch_TIME(&myt);
////							//my_time_t mytt2 = my_system_gmt_sec(&myt, &mytz, &myb);
////							*(int*)out_ptr = (int)(long)secs;
////						} else {
////							*(int*)out_ptr = 0;
////						}
////					}
////				} else if(field->type() == MYSQL_TYPE_DATETIME) {
////					RCValueObject rcvo = rcitem->Value();
////					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(rcvo.Get())) {
////						ulonglong n_val = 0;
////						n_val += rcdt->Year();
////						n_val *= 100;
////						n_val += rcdt->Month();
////						n_val *= 100;
////						n_val += rcdt->Day();
////						n_val *= 100;
////						n_val += rcdt->Hour();
////						n_val *= 100;
////						n_val += rcdt->Minute();
////						n_val *= 100;
////						n_val += rcdt->Second();
////
////						*(longlong*)out_ptr = n_val;
////					}
////				}
////				break;
////			}
////			default:
////				return 0;
////		}
////	}
////	return 1;
////#endif
////}
//
//int RCEngine::Convert(int& is_null, Field* field, void* out_ptr, RCDataType& rcitem)
//{
//#ifdef PURE_LIBRARY
//	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
//	return 0;
//#else
//>>>>>>> .merge-right.r15071
//	if(rcitem.IsNull()) {
//		is_null = 1;
//		return 1;
//	}
//	is_null = 0;
//	bool done = true;
//	switch(field->type()) {
//		case MYSQL_TYPE_VARCHAR: {
//			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<RCBString*>(&rcitem));
//			RCBString& str_val = (RCBString&)rcitem;
//			if(str_val.size() > field->field_length)
//				throw DatabaseRCException("Incorrect field size");
//			if(field->field_length <= 255)
//				str_val.PutVarchar((char*&)out_ptr, 1, false);
//			else if(field->field_length <= ((1 << 16) - 1))
//				str_val.PutVarchar((char*&)out_ptr, 2, false);
//			break;
//		}
//		case MYSQL_TYPE_STRING:
//			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<RCBString*>(&rcitem));
//			((RCBString&)rcitem).PutString((char*&)out_ptr, (ushort)field->field_length, false);
//			break;
//		case MYSQL_TYPE_BLOB: {
//			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<RCBString*>(&rcitem));
//			Field_blob *blob= (Field_blob*) field;
//			if (blob_buf==NULL) {
//				blob->set_ptr(((RCBString&)rcitem).len, (uchar*)((RCBString&)rcitem).val);
//				blob->copy();
//			} else {
//				blob->store(((RCBString&)rcitem).val, ((RCBString&)rcitem).len, &my_charset_bin);
//				uchar *src, *tgt;
//
//				uint packlength= blob->pack_length_no_ptr();
//				uint length= blob->get_length(blob->ptr);
//				memcpy_fixed(&src, blob->ptr + packlength, sizeof(char*));
//				if (src)
//				{
//					blob_buf->resize(length);
//					tgt= &((*blob_buf)[0]);
//					bmove(tgt, src, length);
//					memcpy_fixed(blob->ptr + packlength, &tgt, sizeof(char*));
//				}
//			}
//			break;
//		}
//		case MYSQL_TYPE_DECIMAL:
//		case MYSQL_TYPE_NEWDECIMAL: {
//			my_decimal md;
//			if(rcitem.Type() == RC_REAL)
//				double2decimal((double)((RCNum&)(rcitem)), &md);
//			else
//				RCEngine::Convert(is_null, &md, rcitem);
//			decimal_round(&md, &md, ((Field_new_decimal*)field)->decimals(), HALF_UP);
//			decimal2bin(&md, (uchar*)out_ptr, ((Field_new_decimal*)field)->precision, ((Field_new_decimal*)field)->decimals());
//			break;
//		}
//		default:
//			done = false;
//			break;
//	}
//	if(!done) {
//		is_null = 0;
//		switch(rcitem.Type()) {
//			case RC_BYTEINT:
//			case RC_SMALLINT:
//			case RC_MEDIUMINT:
//			case RC_INT:
//			case RC_BIGINT:
//			case RC_REAL:
//			case RC_FLOAT:
//			case RC_NUM:
//				switch(field->type()) {
//					case MYSQL_TYPE_TINY:
//						*(char*)out_ptr =	(char)(_int64)((RCNum&)(rcitem));
//						break;
//					case MYSQL_TYPE_SHORT:
//						*(short*)out_ptr =	(short)(_int64)((RCNum&)(rcitem));
//						break;
//					case MYSQL_TYPE_INT24:
//						int3store((char*)out_ptr, (int)(_int64)((RCNum&)(rcitem)));
//						break;
//					case MYSQL_TYPE_LONG:
//						*(int*)out_ptr =	(int)(_int64)((RCNum&)(rcitem));
//						break;
//					case MYSQL_TYPE_LONGLONG:
//						*(_int64*)out_ptr =	(_int64)((RCNum&)(rcitem));
//						break;
//					case MYSQL_TYPE_FLOAT:
//						*(float*)out_ptr =	(float)((RCNum&)(rcitem));
//						break;
//					case MYSQL_TYPE_DOUBLE:
//						*(double*)out_ptr = (double)((RCNum&)(rcitem));
//						break;
//					default:
//						BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"No data types conversion available!");
//						break;
//				}
//				break;
//			case RC_STRING:
//				switch(field->type()) {
//					case MYSQL_TYPE_VARCHAR: {
//						RCBString& str_val = (RCBString&)rcitem;
//						if(str_val.size() > field->field_length)
//							throw DatabaseRCException("Incorrect field size");
//						if(field->field_length <= 255) {
//							str_val.PutVarchar((char*&)out_ptr, 1, false);
//						} else if(field->field_length <= ((1 << 16) - 1)) {
//							str_val.PutVarchar((char*&)out_ptr, 2, false);
//						}
//						break;
//					}
//					case MYSQL_TYPE_STRING:
//						((RCBString&)rcitem).PutString((char*&)out_ptr, (ushort)field->field_length, false);
//						break;
//					case MYSQL_TYPE_BLOB: {
//						Field_blob *blob= (Field_blob*) field;
//						if (blob_buf==NULL) {
//							blob->set_ptr(((RCBString&)rcitem).len, (uchar*)((RCBString&)rcitem).val);
//							blob->copy();
//						} else {
//							blob->store(((RCBString&)rcitem).val, ((RCBString&)rcitem).len, &my_charset_bin);
//							uchar *src, *tgt;
//
//							uint packlength= blob->pack_length_no_ptr();
//							uint length= blob->get_length(blob->ptr);
//							memcpy_fixed(&src, blob->ptr + packlength, sizeof(char*));
//							if (src)
//							{
//								blob_buf->resize(length);
//								tgt= &((*blob_buf)[0]);
//								bmove(tgt, src, length);
//								memcpy_fixed(blob->ptr + packlength, &tgt, sizeof(char*));
//							}
//						}
//						break;
//					}
//					case MYSQL_TYPE_DATE: {
//						char tmp[10];
//						char* tmpptr = tmp;
//						((RCBString&)rcitem).PutString(tmpptr, ushort(sizeof(tmp)), false);
//						((Field_date*)field)->store(tmp, sizeof(tmp),NULL);
//						break;
//					}
//					case MYSQL_TYPE_TIME: {
//						char tmp[10];
//						char* tmpptr = tmp;
//						((RCBString&)rcitem).PutString(tmpptr, ushort(sizeof(tmp)), false);
//						((Field_time*)field)->store(tmp,sizeof(tmp),NULL);
//						break;
//					}
//					case MYSQL_TYPE_DATETIME: {
//						char tmp[19];
//						char* tmpptr = tmp;
//						((RCBString&)rcitem).PutString(tmpptr, ushort(sizeof(tmp)), false);
//						((Field_datetime*)field)->store(tmp,sizeof(tmp),NULL);
//						break;
//					}
//					default:
//						((RCBString&)rcitem).PutString((char*&)out_ptr, (ushort)field->field_length, false);
//						break;
//				}
//
//				break;
//			case RC_YEAR: {
//				if(field->type() == MYSQL_TYPE_YEAR) {
//					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
//						int y = rcdt->Year();
//						*(char*)out_ptr = (y != 0 ? y - 1900 : y);
//						if(field->field_length != 2)
//							field->field_length = 4;
//					} else
//						assert(0);
//				} else
//					assert(0);
//				break;
//			}
//			case RC_DATE: {
//				if(field->type() == MYSQL_TYPE_DATE || field->type() == MYSQL_TYPE_NEWDATE) {
//					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
//						uint n_val = 0;
//						if(field->real_type() != MYSQL_TYPE_NEWDATE) {
//							n_val += rcdt->Year();
//							n_val *= 100;
//							n_val += rcdt->Month();
//							n_val *= 100;
//							n_val += rcdt->Day();
//							*(int*)out_ptr = n_val;
//						} else {
//							n_val |= (rcdt->Day()); //day
//							n_val |= (rcdt->Month()) << 5; //mth
//							n_val |= (rcdt->Year()) << 9; //year
//							int3store((char*)out_ptr, n_val);
//						}
//					} else
//						assert(0);
//				}
//				break;
//			}
//			case RC_TIME: {
//				if(field->type() == MYSQL_TYPE_TIME) {
//					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
//						long n_val = rcdt->Hour();
//						n_val *= 100;
//						n_val += rcdt->Minute();
//						n_val *= 100;
//						n_val += rcdt->Second();
//						int3store((char*)out_ptr, n_val);
//					} else
//						assert(0);
//				}
//				break;
//			}
//			case RC_DATETIME: {
//				if(field->type() == MYSQL_TYPE_DATETIME) {
//					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
//						ulonglong n_val = 0;
//						n_val += rcdt->Year();
//						n_val *= 100;
//						n_val += rcdt->Month();
//						n_val *= 100;
//						n_val += rcdt->Day();
//						n_val *= 100;
//						n_val += rcdt->Hour();
//						n_val *= 100;
//						n_val += rcdt->Minute();
//						n_val *= 100;
//						n_val += rcdt->Second();
//
//						*(longlong*)out_ptr = n_val;
//					}
//				}
//				break;
//			}
//			case RC_TIMESTAMP: {
//				if(field->type() == MYSQL_TYPE_TIMESTAMP) {
//					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
//						if(*rcdt != RC_TIMESTAMP_SPEC) {
//							MYSQL_TIME myt;
//							myt.year = rcdt->Year();
//							myt.month = rcdt->Month();
//							myt.day = rcdt->Day();
//							myt.hour = rcdt->Hour();
//							myt.minute = rcdt->Minute();
//							myt.second = rcdt->Second();
//							myt.time_type = MYSQL_TIMESTAMP_DATETIME;
//							my_time_t secs = sec_since_epoch_TIME(&myt);
//							//my_time_t mytt2 = my_system_gmt_sec(&myt, &mytz, &myb);
//							*(int*)out_ptr = (int)(long)secs;
//						} else {
//							*(int*)out_ptr = 0;
//						}
//					}
//				} else if(field->type() == MYSQL_TYPE_DATETIME) {
//					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
//						ulonglong n_val = 0;
//						n_val += rcdt->Year();
//						n_val *= 100;
//						n_val += rcdt->Month();
//						n_val *= 100;
//						n_val += rcdt->Day();
//						n_val *= 100;
//						n_val += rcdt->Hour();
//						n_val *= 100;
//						n_val += rcdt->Minute();
//						n_val *= 100;
//						n_val += rcdt->Second();
//
//						*(longlong*)out_ptr = n_val;
//					}
//				}
//				break;
//			}
//			default:
//				return 0;
//		}
//	}
//	return 1;
//#endif
//}


int RCEngine::Convert(int& is_null, my_decimal* value, RCDataType& rcitem, int output_scale)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(rcitem.IsNull())
		is_null = 1;
	else {
		if(!RCEngine::AreConvertible(rcitem, MYSQL_TYPE_NEWDECIMAL))
			return false;
		is_null = 0;
		if(rcitem.Type() == RC_NUM) {
			RCNum* rcn = (RCNum*)(&rcitem);
			int intg = rcn->GetDecIntLen();
			int frac = rcn->GetDecFractLen();
			int intg1 = ROUND_UP(intg);
			int frac1 = ROUND_UP(frac);
			value->intg = intg;
			value->frac = frac;
			_int64 ip = rcn->GetIntPart();
			_int64 fp = (rcn->Value() % (_int64)Uint64PowOfTen(rcn->Scale()));
			bool special_value_minbigint = false;
			if(_uint64(ip) == 0x8000000000000000ULL) {
				// a special case, cannot be converted like that
				special_value_minbigint = true;
				ip += 1;	// just for now...
			}
			if(ip < 0) {
				ip *= -1;
				value->sign(true);
				if(fp < 0)
					fp *= -1;
			} else if(ip == 0 && fp < 0) {
				fp *= -1;
				value->sign(true);
			} else
				value->sign(false);

			decimal_digit_t* buf = value->buf + intg1;
			for(int i = intg1; i > 0; i--) {
				*--buf = decimal_digit_t(ip % DIG_BASE);
				if(special_value_minbigint && i == intg1) {
					*buf += 1;		// revert the special case (plus, because now it is unsigned part)
				}
				ip /= DIG_BASE;
			}
			buf = value->buf + intg1 + (frac1 - 1);
			_int64 tmp(fp);
			int no_digs = 0;
			while(tmp > 0) {
				tmp /= 10;
				no_digs++;
			}
			int tmp_prec = rcn->Scale();

			for(; frac1; frac1--) {
				int digs_to_take = tmp_prec - (frac1 - 1) * DIG_PER_DEC1;
				if(digs_to_take < 0)
					digs_to_take = 0;
				tmp_prec -= digs_to_take;
				int cur_pow = DIG_PER_DEC1 - digs_to_take;
				*buf-- = decimal_digit_t((fp % (_int64)Uint64PowOfTen(digs_to_take)) * (_int64)Uint64PowOfTen(cur_pow));
				fp /= (_int64)Uint64PowOfTen(digs_to_take);
			}
			int output_scale_1 = (output_scale > 18) ? 18 : output_scale; 				
			my_decimal_round(0, value, (output_scale_1 == -1) ? frac : output_scale_1, false, value);
			return 1;
		} else if(rcitem.Type() == RC_REAL || rcitem.Type() == RC_FLOAT) {
			double2decimal((double)((RCNum&)(rcitem)), (decimal_t*)value);
			return 1;
		} else if(ATI::IsIntegerType(rcitem.Type())) {
				longlong2decimal((longlong)((RCNum&)(rcitem)).value, (decimal_t*)value);
			return 1;
		}
		return false;
	}
	return 1;
#endif
}

int RCEngine::Convert(int& is_null, _int64& value, RCDataType& rcitem, enum_field_types f_type)
{
	if(rcitem.IsNull())
		is_null = 1;
	else {
		is_null = 0;
		if(rcitem.Type() == RC_NUM || rcitem.Type() == RC_BIGINT) {			
			value = (_int64)(RCNum&)rcitem;
			switch(f_type) {
				case MYSQL_TYPE_LONG : 
				case MYSQL_TYPE_INT24 : 
					if(value == NULL_VALUE_64)
						value = NULL_VALUE_32;
					else if(value == PLUS_INF_64)
						value = BH_INT_MAX;
					else if(value == MINUS_INF_64)
						value = BH_INT_MIN;	
					break;
				case MYSQL_TYPE_TINY: 
					if(value == NULL_VALUE_64)
						value = NULL_VALUE_C;
					else if(value == PLUS_INF_64)
						value = BH_TINYINT_MAX;
					else if(value == MINUS_INF_64)
						value = BH_TINYINT_MIN;	
					break;
				case MYSQL_TYPE_SHORT: 
					if(value == NULL_VALUE_64)
						value = NULL_VALUE_SH;
					else if(value == PLUS_INF_64)
						value = BH_SMALLINT_MAX;
					else if(value == MINUS_INF_64)
						value = BH_SMALLINT_MIN;	
					break;
			}
			return 1;
		} else if(rcitem.Type() == RC_INT || rcitem.Type() == RC_MEDIUMINT) {
			value = (int)(_int64)dynamic_cast<RCNum&>(rcitem);
			return 1;
		} else if(rcitem.Type() == RC_BYTEINT) {
			value = (char)(_int64)dynamic_cast<RCNum&>(rcitem);
			return 1;
		} else if(rcitem.Type() == RC_SMALLINT) {
			value = (short)(_int64)dynamic_cast<RCNum&>(rcitem);
			return 1;
		} else if(rcitem.Type() == RC_YEAR) {
			value = dynamic_cast<RCDateTime&>(rcitem).Year();
			return 1;
		} else if(rcitem.Type() == RC_REAL) {
			value = (_int64)(double)dynamic_cast<RCNum&>( rcitem );
			return 1;
		}
	}
	return 0;
}

//<<<<<<< .working
//int RCEngine::Convert(int& is_null, _int64& value, RCDataType& rcitem, enum_field_types f_type)
//=======
//int RCEngine::Convert(int& is_null, my_decimal* value, RCItem* rcitem, int output_scale)
//>>>>>>> .merge-right.r15071
//{
//<<<<<<< .working
//	if(rcitem.IsNull())
//=======
//#ifdef PURE_LIBRARY
//	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
//	return 0;
//#else
//	if(rcitem->IsNull())
//>>>>>>> .merge-right.r15071
//		is_null = 1;
//	else {
//		is_null = 0;
//		if(rcitem.Type() == RC_NUM || rcitem.Type() == RC_BIGINT) {			
//			value = (_int64)(RCNum&)rcitem;
//			switch(f_type) {
//				case MYSQL_TYPE_LONG : 
//				case MYSQL_TYPE_INT24 : 
//					if(value == NULL_VALUE_64)
//						value = NULL_VALUE_32;
//					else if(value == PLUS_INF_64)
//						value = BH_INT_MAX;
//					else if(value == MINUS_INF_64)
//						value = BH_INT_MIN;	
//					break;
//				case MYSQL_TYPE_TINY: 
//					if(value == NULL_VALUE_64)
//						value = NULL_VALUE_C;
//					else if(value == PLUS_INF_64)
//						value = BH_TINYINT_MAX;
//					else if(value == MINUS_INF_64)
//						value = BH_TINYINT_MIN;	
//					break;
//				case MYSQL_TYPE_SHORT: 
//					if(value == NULL_VALUE_64)
//						value = NULL_VALUE_SH;
//					else if(value == PLUS_INF_64)
//						value = BH_SMALLINT_MAX;
//					else if(value == MINUS_INF_64)
//						value = BH_SMALLINT_MIN;	
//					break;
//			}
//			int output_scale_1 = (output_scale > 18) ? 18 : output_scale; 				
//			my_decimal_round(0, value, (output_scale_1 == -1) ? frac : output_scale_1, false, value);
//			return 1;
//		} else if(rcitem.Type() == RC_INT || rcitem.Type() == RC_MEDIUMINT) {
//			value = (int)(_int64)dynamic_cast<RCNum&>(rcitem);
//			return 1;
//		} else if(rcitem.Type() == RC_BYTEINT) {
//			value = (char)(_int64)dynamic_cast<RCNum&>(rcitem);
//			return 1;
//		} else if(rcitem.Type() == RC_SMALLINT) {
//			value = (short)(_int64)dynamic_cast<RCNum&>(rcitem);
//			return 1;
//		} else if(rcitem.Type() == RC_YEAR) {
//			value = dynamic_cast<RCDateTime&>(rcitem).Year();
//			return 1;
//		} else if(rcitem.Type() == RC_REAL) {
//			value = (_int64)(double)dynamic_cast<RCNum&>( rcitem );
//			return 1;
//		}
//	}
//	return 0;
//}

int RCEngine::Convert(int& is_null, double& value, RCDataType& rcitem)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(rcitem.IsNull())
		is_null = 1;
	else {
		if(!RCEngine::AreConvertible(rcitem, MYSQL_TYPE_DOUBLE))
			return 0;
		is_null = 0;
		if(rcitem.Type() == RC_REAL) {
			value = (double)dynamic_cast<RCNum&>( rcitem );
			return 1;
		} else if(rcitem.Type() == RC_FLOAT) {
			value = (float)dynamic_cast<RCNum&>( rcitem );
			return 1;
		}
	}
	return 0;
#endif
}

int RCEngine::Convert(int& is_null, String* value, RCDataType& rcitem, enum_field_types f_type)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(rcitem.IsNull())
		is_null = 1;
	else {
		if(!RCEngine::AreConvertible(rcitem, MYSQL_TYPE_STRING))
			return 0;
		is_null = 0;
		if(f_type == MYSQL_TYPE_VARCHAR || f_type == MYSQL_TYPE_VAR_STRING) {
			RCBString str = rcitem.ToRCString();
			value->set_ascii(str.val, str.len);
			value->copy();
		} else if(f_type == MYSQL_TYPE_STRING) {
			RCBString str = rcitem.ToRCString();
			value->set_ascii(str.val, str.len);
			value->copy();
		} else if(f_type == MYSQL_TYPE_NEWDATE || f_type == MYSQL_TYPE_DATE) {
			RCBString str = rcitem.ToRCString();
			value->set_ascii(str.val, str.len);
			value->copy();
		} else if(f_type == MYSQL_TYPE_TIME) {
			RCBString str = rcitem.ToRCString();
			value->set_ascii(str.val, str.len);
			value->copy();
		} else if(f_type == MYSQL_TYPE_DATETIME) {
			RCBString str = rcitem.ToRCString();
			value->set_ascii(str.val, str.len);
			value->copy();
		} else if(f_type == MYSQL_TYPE_TIMESTAMP) {
			if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(&rcitem)) {
				if(*rcdt != RC_TIMESTAMP_SPEC) {
					MYSQL_TIME myt, local_time;
					myt.year = rcdt->Year();
					myt.month = rcdt->Month();
					myt.day = rcdt->Day();
					myt.hour = rcdt->Hour();
					myt.minute = rcdt->Minute();
					myt.second = rcdt->Second();
					myt.time_type = MYSQL_TIMESTAMP_DATETIME;
					my_time_t secs = sec_since_epoch_TIME(&myt);
					ConnectionInfoOnTLS.Get().Thd().variables.time_zone->gmt_sec_to_TIME(&local_time, secs);
					char buf[20];
					my_datetime_to_str(&local_time, buf);
					value->set_ascii(buf, 19);
				} else {
					value->set_ascii("0000-00-00 00:00:00", 19);
				}
			} else {
				RCBString str = rcitem.ToRCString();
				value->set_ascii(str.val, str.len);
			}
			value->copy();
		} else if(f_type == MYSQL_TYPE_BLOB) {
			RCBString str = rcitem.ToRCString();
			value->set_ascii(str.val, str.len);
			value->copy();
		}
		return 1;
	}
	return 0;
#endif
}

bool RCEngine::AreConvertible(RCDataType& rcitem, enum_field_types my_type, uint length)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	/*if(rcitem->Type() == RCEngine::GetCorrespondingType(my_type, length) || rcitem->IsNull())
	 return true;*/
	AttributeType bhtype = rcitem.Type();
	switch(my_type) {
		case MYSQL_TYPE_LONGLONG:
			if(bhtype == RC_INT || bhtype == RC_MEDIUMINT || bhtype == RC_BIGINT || (bhtype == RC_NUM && dynamic_cast<RCNum&>(rcitem).Scale() == 0))
				return true;
			break;
		case MYSQL_TYPE_NEWDECIMAL:
			if(bhtype == RC_FLOAT || bhtype == RC_REAL || ATI::IsIntegerType(bhtype) || bhtype == RC_NUM)
				return true;
			break;
		case MYSQL_TYPE_BLOB:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
			return (bhtype == RC_STRING || bhtype == RC_VARCHAR || bhtype == RC_BYTE || bhtype == RC_VARBYTE || bhtype
					== RC_BIN);
		case MYSQL_TYPE_YEAR:
			return bhtype == RC_YEAR;
		case MYSQL_TYPE_SHORT:
			return bhtype == RC_SMALLINT;
		case MYSQL_TYPE_TINY:
			return bhtype == RC_BYTEINT;
		case MYSQL_TYPE_INT24:
			return bhtype == RC_MEDIUMINT;
		case MYSQL_TYPE_LONG:
			return bhtype == RC_INT;
		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
			return bhtype == RC_FLOAT || bhtype == RC_REAL;
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_DATETIME:
			return (bhtype == RC_DATETIME || bhtype == RC_TIMESTAMP);
		case MYSQL_TYPE_TIME:
			return bhtype == RC_TIME;
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATE:
			return bhtype == RC_DATE;
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_VAR_STRING:
			return true;
	}
	return false;
#endif
}
//
//
//bool RCEngine::AreConvertible(RCDataType& rcitem, enum_field_types my_type, uint length)
//{
//#ifdef PURE_LIBRARY
//	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
//<<<<<<< .working
//=======
//	return false;
//#else
//	if(rcitem->Type() == RCEngine::GetCorrespondingType(*field) || rcitem->IsNull())
//		return true;
//	switch(field->type()) {
//		case MYSQL_TYPE_TINY:
//		case MYSQL_TYPE_SHORT:
//		case MYSQL_TYPE_INT24:
//		case MYSQL_TYPE_LONG:
//		case MYSQL_TYPE_LONGLONG:
//			return ATI::IsIntegerType(rcitem->Type()) || ATI::IsRealType(rcitem->Type()) || (rcitem->Type() == RC_NUM && ((RCNum*)(rcitem->Value().Get()))->Scale() == 0);
//		case MYSQL_TYPE_VARCHAR:
//		case MYSQL_TYPE_VAR_STRING:
//			return (rcitem->Type() == RC_STRING);
//		case MYSQL_TYPE_NEWDECIMAL:
//			if(ATI::IsRealType(rcitem->Type()))
//				return true;
//			else
//				return (rcitem->Type() == RC_INT || rcitem->Type() == RC_MEDIUMINT || rcitem->Type() == RC_BIGINT);
//		case MYSQL_TYPE_STRING:
//			return (ATI::IsDateTimeType(rcitem->Type()) || rcitem->Type() == RC_STRING);
//		case MYSQL_TYPE_BLOB:
//			return (rcitem->Type() == RC_STRING || rcitem->Type() == RC_NUM);
//		case MYSQL_TYPE_FLOAT:
//		case MYSQL_TYPE_DOUBLE:
//			return rcitem->Type() == RC_FLOAT || rcitem->Type() == RC_REAL;
//		case MYSQL_TYPE_DATETIME:
//		case MYSQL_TYPE_TIMESTAMP:
//			return (rcitem->Type() == RC_TIMESTAMP) || (rcitem->Type() == RC_DATETIME) || (rcitem->Type() == RC_STRING);
//		case MYSQL_TYPE_DATE:
//			return (rcitem->Type() == RC_DATE)|| (rcitem->Type() == RC_STRING);
//		case MYSQL_TYPE_TIME:
//			return (rcitem->Type() == RC_TIME)|| (rcitem->Type() == RC_STRING);
//		default:
//			return false;
//	}
//>>>>>>> .merge-right.r15071
//	return false;
//<<<<<<< .working
//=======
//#endif
//}
//
//bool RCEngine::AreConvertible(RCItem* rcitem, enum_field_types my_type, uint length)
//{
//#ifdef PURE_LIBRARY
//	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
//	return false;
//>>>>>>> .merge-right.r15071
//#else
//	/*if(rcitem->Type() == RCEngine::GetCorrespondingType(my_type, length) || rcitem->IsNull())
//	 return true;*/
//	AttributeType bhtype = rcitem.Type();
//	switch(my_type) {
//		case MYSQL_TYPE_LONGLONG:
//			if(bhtype == RC_INT || bhtype == RC_MEDIUMINT || bhtype == RC_BIGINT || (bhtype == RC_NUM && dynamic_cast<RCNum&>(rcitem).Scale() == 0))
//				return true;
//			break;
//		case MYSQL_TYPE_NEWDECIMAL:
//			if(bhtype == RC_FLOAT || bhtype == RC_REAL || ATI::IsIntegerType(bhtype) || bhtype == RC_NUM)
//				return true;
//			break;
//		case MYSQL_TYPE_BLOB:
//		case MYSQL_TYPE_TINY_BLOB:
//		case MYSQL_TYPE_MEDIUM_BLOB:
//		case MYSQL_TYPE_LONG_BLOB:
//			return (bhtype == RC_STRING || bhtype == RC_VARCHAR || bhtype == RC_BYTE || bhtype == RC_VARBYTE || bhtype
//					== RC_BIN);
//		case MYSQL_TYPE_YEAR:
//			return bhtype == RC_YEAR;
//		case MYSQL_TYPE_SHORT:
//			return bhtype == RC_SMALLINT;
//		case MYSQL_TYPE_TINY:
//			return bhtype == RC_BYTEINT;
//		case MYSQL_TYPE_INT24:
//			return bhtype == RC_MEDIUMINT;
//		case MYSQL_TYPE_LONG:
//			return bhtype == RC_INT;
//		case MYSQL_TYPE_FLOAT:
//		case MYSQL_TYPE_DOUBLE:
//			return bhtype == RC_FLOAT || bhtype == RC_REAL;
//		case MYSQL_TYPE_TIMESTAMP:
//		case MYSQL_TYPE_DATETIME:
//			return (bhtype == RC_DATETIME || bhtype == RC_TIMESTAMP);
//		case MYSQL_TYPE_TIME:
//			return bhtype == RC_TIME;
//		case MYSQL_TYPE_NEWDATE:
//		case MYSQL_TYPE_DATE:
//			return bhtype == RC_DATE;
//		case MYSQL_TYPE_VARCHAR:
//		case MYSQL_TYPE_STRING:
//		case MYSQL_TYPE_VAR_STRING:
//			return true;
//	}
//	return false;
//#endif
//}

AttributeType RCEngine::GetCorrespondingType(const enum_field_types& eft)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return RC_INT;
#else
	switch(eft) {
		case MYSQL_TYPE_YEAR:
			return RC_YEAR;
		case MYSQL_TYPE_SHORT:
			return RC_SMALLINT;
		case MYSQL_TYPE_TINY:
			return RC_BYTEINT;
		case MYSQL_TYPE_INT24:
			return RC_MEDIUMINT;
		case MYSQL_TYPE_LONG:
			return RC_INT;
		case MYSQL_TYPE_LONGLONG:
			return RC_BIGINT;
		case MYSQL_TYPE_FLOAT:
			return RC_FLOAT;
		case MYSQL_TYPE_DOUBLE:
			return RC_REAL;
		case MYSQL_TYPE_TIMESTAMP:
			return RC_TIMESTAMP;
		case MYSQL_TYPE_DATETIME:
			return RC_DATETIME;
		case MYSQL_TYPE_TIME:
			return RC_TIME;
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATE:
			return RC_DATE;
		case MYSQL_TYPE_NEWDECIMAL:
			return RC_NUM;
		case MYSQL_TYPE_STRING:
			return RC_STRING;
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_BLOB :
			return RC_VARCHAR;
		default :
			return RC_UNKNOWN;
	}
#endif
}

AttributeType RCEngine::GetCorrespondingType(const Field& field)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return RC_INT;
#else
	AttributeType res = GetCorrespondingType(field.type());
	if(!ATI::IsStringType(res))
		return res;
	else {
		switch(field.type()) {
			case MYSQL_TYPE_STRING:
			case MYSQL_TYPE_VARCHAR:
			case MYSQL_TYPE_VAR_STRING:
			{
				if(const Field_str* fstr = dynamic_cast<const Field_string*>(&field))
				{
					if(fstr->charset() != &my_charset_bin)
						return RC_STRING;
					return RC_BYTE;
				}
				else if(const Field_str* fvstr = dynamic_cast<const Field_varstring*>(&field))
				{
					if(fvstr->charset() != &my_charset_bin)
						return RC_VARCHAR;
					return RC_VARBYTE;

				}
			}
			break;
			case MYSQL_TYPE_BLOB :
			if(const Field_str* fstr = dynamic_cast<const Field_str*>(&field))
			{
				if(const Field_blob* fblo = dynamic_cast<const Field_blob*>(fstr))
				{
					if(fblo->charset() != &my_charset_bin)
					{//TINYTEXT, MEDIUMTEXT, TEXT, LONGTEXT
						return RC_VARCHAR;
					}
					else
					{
						switch(field.field_length)
						{
							case 255 :
							case 65535 :
							//TINYBLOB, BLOB
							return RC_VARBYTE;
							case 16777215 :
						case (size_t)4294967295UL :
							//MEDIUMBLOB, LONGBLOB
							return RC_BIN;
						}
					}
				}
			}
			break;
			default :
				return RC_UNKNOWN;
		}
	}
	return RC_UNKNOWN;
#endif
}

ATI RCEngine::GetCorrespondingATI(Field& field)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return ATI(RC_INT, true);
#else
	AttributeType at = GetCorrespondingType(field);

	if(ATI::IsNumericType(at)) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<Field_num*>(&field));
		if(at == RC_NUM) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<Field_new_decimal*>(&field));
			return ATI(at, !field.maybe_null(), static_cast<Field_new_decimal&>(field).precision, static_cast<Field_num&>(field).decimals());
		}
		return ATI(at, !field.maybe_null(), field.field_length, static_cast<Field_num&>(field).decimals());
	}
	return ATI(at, !field.maybe_null(), field.field_length);

#endif
}
