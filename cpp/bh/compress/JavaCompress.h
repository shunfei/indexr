#ifndef __JAVA_COMPRESS_H
#define __JAVA_COMPRESS_H

// Interface for Java.
// This file should avoid include any files, to avoid generating massive unused code. Use basic c++ type here.

// Copied from defs.h.
enum CprsErr_J {
	CPRS_SUCCESS_J = 0,
	CPRS_ERR_BUF_J = 1, 	// buffer overflow error
	CPRS_ERR_PAR_J = 2,		// bad parameters
	CPRS_ERR_SUM_J = 3,		// wrong cumulative-sum table (for arithmetic coding)
	CPRS_ERR_VER_J = 4,		// incorrect version of compressed data
	CPRS_ERR_COR_J = 5,		// compressed data are corrupted
	CPRS_ERR_MEM_J = 6,		// memory allocation error
	CPRS_ERR_OTH_J = 100	// other error (unrecognized)
};

#ifndef _java_int_
typedef unsigned int java_int;
#endif

#ifndef _java_long_
typedef unsigned long long java_long;
#endif

extern "C" void Test(java_long a);

extern "C" void* JavaCompress_Number_Byte(void* data, java_int item_size, java_long max_val);
extern "C" void* JavaCompress_Number_Short(void* data, java_int item_size, java_long max_val);
extern "C" void* JavaCompress_Number_Int(void* data, java_int item_size, java_long max_val);
extern "C" void* JavaCompress_Number_Long(void* data, java_int item_size, java_long max_val);

extern "C" void* JavaDecompress_Number_Byte(void* cmp_data, java_int item_size);
extern "C" void* JavaDecompress_Number_Short(void* cmp_data, java_int item_size);
extern "C" void* JavaDecompress_Number_Int(void* cmp_data, java_int item_size);
extern "C" void* JavaDecompress_Number_Long(void* cmp_data, java_int item_size);

extern "C" void* JavaCompress_IndexedString(void* data, java_int item_size);
extern "C" void* JavaDecompress_IndexedString(void* cmp_data, java_int item_size);

extern "C" void* JavaCompress_IndexedString_v1(void* data, java_int item_size);
extern "C" void* JavaDecompress_IndexedString_v1(void* cmp_data, java_int item_size);

// Interfaces without pointer (void*) are slightly faster than pointer version.

extern "C" java_long JavaCompress_Number_Byte_NP(java_long data, java_int item_size, java_long max_val){
	return (java_long)JavaCompress_Number_Byte((void*)data, item_size, max_val);
}
extern "C" java_long JavaCompress_Number_Short_NP(java_long data, java_int item_size, java_long max_val){
	return (java_long)JavaCompress_Number_Short((void*)data, item_size, max_val);
}
extern "C" java_long JavaCompress_Number_Int_NP(java_long data, java_int item_size, java_long max_val){
	return (java_long)JavaCompress_Number_Int((void*)data, item_size, max_val);
}
extern "C" java_long JavaCompress_Number_Long_NP(java_long data, java_int item_size, java_long max_val){
	return (java_long)JavaCompress_Number_Long((void*)data, item_size, max_val);
}

extern "C" java_long JavaDecompress_Number_Byte_NP(java_long cmp_data, java_int item_size){
	return (java_long)JavaDecompress_Number_Byte((void*)cmp_data, item_size);
}
extern "C" java_long JavaDecompress_Number_Short_NP(java_long cmp_data, java_int item_size){
	return (java_long)JavaDecompress_Number_Short((void*)cmp_data, item_size);
}
extern "C" java_long JavaDecompress_Number_Int_NP(java_long cmp_data, java_int item_size){
	return (java_long)JavaDecompress_Number_Int((void*)cmp_data, item_size);
}
extern "C" java_long JavaDecompress_Number_Long_NP(java_long cmp_data, java_int item_size){
	return (java_long)JavaDecompress_Number_Long((void*)cmp_data, item_size);
}

extern "C" java_long JavaCompress_IndexedString_NP(java_long data, java_int item_size){
	return (java_long)JavaCompress_IndexedString((void*)data, item_size);
}
extern "C" java_long JavaDecompress_IndexedString_NP(java_long cmp_data, java_int item_size){
	return (java_long)JavaDecompress_IndexedString((void*)cmp_data, item_size);
}

extern "C" java_long JavaCompress_IndexedString_NP_v1(java_long data, java_int item_size){
	return (java_long)JavaCompress_IndexedString_v1((void*)data, item_size);
}
extern "C" java_long JavaDecompress_IndexedString_NP_v1(java_long cmp_data, java_int item_size){
	return (java_long)JavaDecompress_IndexedString_v1((void*)cmp_data, item_size);
}

#endif
