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

#ifndef _QUERY_OPERATOR_H_
#define _QUERY_OPERATOR_H_

#include <string>

/**
	The base object representation of a Query Operator.

 	Further use will require the further definition of this base
	class' functionality and pure virtual member interfaces but as
	a basic example of this stub's intended use:

		class NotEqualQueryOperator : public QueryOperator
 		{
			public:
				NotEqualQueryOperator():QueryOperator(O_NOT_EQ, "<>") {}
		}

 */

class QueryOperator
{
	public:
		/// Instantiates the query operator object representation.
		/// \param Fetch the Operator enumeration type this object represents.
		/// \param Fetch the string representation of this query operator.
		QueryOperator(Operator t, const std::string& sr) : type(t), string_rep(sr) {}

		/// Fetch the Operator enumeration type this object represents.
		Operator GetType() const { return type; }

		/// Fetch the string representation of this query operator.
		std::string AsString() const { return string_rep; }

	protected:
		/// The Operator enumeration type this object represents.
		Operator type;

		/// A string representation of the query operator.  This string
		/// reflects what would have been enter in the originating SQL
		/// statement.
		std::string string_rep;
};

#endif // _QUERY_OPERATOR_H_

