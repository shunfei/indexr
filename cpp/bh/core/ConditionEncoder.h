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

#ifndef CONDITIONENCODER_H_
#define CONDITIONENCODER_H_

#include <iostream>
#include "common/CommonDefinitions.h"
#include "RCAttr.h"

class RCAttr;
class ValueSet;
class MultiValColumn;

namespace cq2 {
class TempTable;
}

class Descriptor;

class ConditionEncoder
{

public:
	ConditionEncoder(bool additional_nulls);
	virtual ~ConditionEncoder();

	void operator()(Descriptor& desc);
private:

    void DoEncode();
    bool IsTransformationNeeded();
    void TransformINs();
    void TransformOtherThanINsOnNotLookup();
    void TransformLIKEs();
    void TextTransformation();

    void EncodeConditionOnStringColumn();
    void EncodeConditionOnNumerics();
    void TransformWithRespectToNulls();
    void DescriptorTransformation();

    void PrepareValueSet(MultiValColumn& mvc);
    void TransformINsOnLookup();
    void TransformLIKEsPattern();
    void TransformLIKEsIntoINsOnLookup();
    void TransformIntoINsOnLookup();
    void TransformOtherThanINsOnNumerics();
	void LookupExpressionTransformation();

    inline AttributeType AttrTypeName() const { return attr->TypeName(); }

public:
	static void EncodeIfPossible(Descriptor& desc, bool for_rough_query, bool additional_nulls);

private:
	bool 				additional_nulls;
	ColumnType			in_type;
	bool 				sharp;
	bool				encoding_done;
	RCAttr*				attr;
	Descriptor*			desc;
};

#endif /* CONDITIONENCODER_H_ */
