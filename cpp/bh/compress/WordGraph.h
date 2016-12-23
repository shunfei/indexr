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

#ifndef __COMPRESS_WORDGRAPH_H
#define __COMPRESS_WORDGRAPH_H

#include <vector>
#include <cstring>
#include <iostream>
#include <math.h>
#include "PPMdefs.h"
#include "common/bhassert.h"

//#define SHADOW_EDGES
//#define EXP_ESC_COUNT


class WordGraph : public PPMModel
{
	typedef	int PNode;
	typedef int PEdge;
	typedef int PSEdge;

#ifdef SOLARIS
#   pragma pack(1)
#else
#   pragma pack(push, 1)
#endif
	struct Node {
		int endpos;		// position of the first symbol _after_ the solid edge label
		int stop;		// no. of occurences of representative of this node as a suffix (so terminating in this node)
		PNode suf;
		PEdge edge;		// pointer to the first edge leaving this node (or ENIL if no edge leaves the node)

		int count;		// 'stop' is included in 'count'
		Count total;	// total count for distribution of outgoing edges + ESC

		//bool istab;		// "edge" points to an array of 256 edges

#		ifdef SHADOW_EDGES
		Symb nshadow;	// no. of shadow edges
		PSEdge sedge;	// first shadow edge
		Count stotal;	// total count for distribution of shadow edges + ESC
#		endif

		Node() { memset(this, 0, sizeof *this); }
	};
	struct Edge {
	private:
		int len;		// edge length is used also to mark if the edge is solid (len>0) or not (len<0)
	public:
		Symb fsym;
		PNode n;		// target node of the edge
		PEdge next;		// next edge in the list

		Count sum;		// lower bound of the edge's range

		Edge()						{ memset(this, 0, sizeof *this); }
		void SetLen(int a)			{ if(len >= 0) len = abs(a); else len = -abs(a); }
		void SetSolid(bool s)		{ if(s) len = abs(len); else len = -abs(len); }
		uint  GetLen()				{ return (len > 0) ? (uint)len : (uint)(-len); }
		bool IsSolid()				{ return len > 0; }
	};
	struct SEdge {		// shadow edge
		Symb fsym;
		Count sum;
	};
#ifdef SOLARIS
#   pragma pack()
#else
#   pragma pack(pop)
#endif

	static const PNode ROOT1 = 1;
	static const PNode NIL = 0;
	static const PEdge ENIL = 0;
	static const PSEdge SENIL = 0;

	std::vector<Node> nodes;
	std::vector<Edge> edges;
	std::vector<PNode> finals;		// list of final nodes
	std::vector<SEdge> sedges;

	// original string - for reference; this is a pointer to _original_ data, not a copy!!!
	const Symb* data;
	int dlen;		// length of 'data', INcluding the possible terminating symbol ('\0')

	PPMParam param;

	//-------------------------------------------------------------------------

	Node& GN(PNode n)			{ return nodes[n]; }
	Edge& GE(PEdge e)			{ return edges[e]; }
	SEdge& GSE(PSEdge e)		{ return sedges[e]; }
	PEdge NxtEdge(PEdge e)		{ return GE(e).next; }

	PEdge FindEdge(PNode n, Symb s);
	PEdge AddEdge(PNode n, Symb s, int len, bool solid, PNode m);
	void CopyEdge(PNode e1, PNode e2)	{ memcpy(&GE(e2), &GE(e1), sizeof(Edge)); }		// copy from e1 to e2
	PSEdge FindSEdge(PNode n, Symb s);

	PNode NewNode()			{ int s = (int)nodes.size(); nodes.resize(s+1); return s; }
	PEdge NewEdge()			{ int s = (int)edges.size(); edges.resize(s+1); return s; }
	//PSEdge NewSEdge()		{ int s = (int)sedges.size(); sedges.resize(s+1); return s; }
	PNode NewFinal(int endpos);

	//void SortEdges(Node& n);

	//-------------------------------------------------------------------------

	void Init();
	void Create();
	void Clear();

	// Point exactly at node 'x' may be represented in 2 ways:
	//  (1) { n = x, proj = 0, edge = undefined }
	//  (2) { n = parent_of_x, proj = len_of_edge, edge = edge_to_x }
	// The latter (not fully canonized) is required during duplication, to find non-solid edges for redirection.
	struct Point {
		PNode n;
		uint proj;		// number of symbols passed from 'n' along 'edge'
		PEdge edge;		// current edge
	};

	void MoveDown(Point& p, const uchar*& s, const uchar* sstop);
	void Duplicate(Point& p);

	// if canonlast=true, 'p' upon exit is in form (1); otherwise it's in form (2)
	void MoveSuffix(Point& p, const uchar* s, bool canonlast);

	void Insert(Point p, int pos, int endpos, PNode& last, PNode& final);

	//-------------------------------------------------------------------------
	// transformation for PPM

	// needed for derecursivation
	struct StackElem {
		PNode n;
		bool proc;		// are children of 'n' already processed?
		StackElem(PNode n_, bool proc_) { n=n_; proc=proc_; }
	};

	int shlen[NSymb];		// used in SetShadow() to store lenghts of edges of the longer-suffix node

	void PropagateStop();
	void SetCounts();
	//void SortEdges();
	void SetSums();
	void SetShadow(PNode pn);
	int Shift(int c);		// c - cumulative count of outgoing edges (without ESC and 'stop' of the node)

	Count GetEscCount(PNode n, int c = 0);
	Count GetShEscCount(PNode n, int c = 0);
	//Count GetEscCount(PNode n)	{ BHASSERT_WITH_NO_PERFORMANCE_IMPACT(n != NIL); return param.esc_count; }
	//Count GetShEscCount(PNode n)	{ BHASSERT_WITH_NO_PERFORMANCE_IMPACT(n != NIL); return param.esc_count; }

	//-------------------------------------------------------------------------
	// PPM compression/decompression

#	ifdef SHADOW_EDGES
	bool NoShadow(PNode n)		{ return GN(n).stotal == 0; }
#	else
	bool NoShadow(PNode n)		{ return true; }
#	endif

	// if the last transition was forward or the last node doesn't have shadow edges, prev == NIL;
	// otherwise it's the previous node
	struct State {
		bool lastesc;	// the last transition was ESC
		PNode prev, n;
	};
	State state;
	//PNode state;

	// when "escape", e := ENIL
	void FindEdge(PEdge& e, PSEdge& se, Symb* str, int len);		// compression
	void FindEdge(PEdge& e, PSEdge& se, Count c);					// decompression

	CprsErr GetLabel(PEdge e, Symb* lbl, int& len);		// 'len' - max size of lbl; upon exit: length of lbl
	void GetRange(PEdge e, PSEdge se, Range& r);		// 'e' must be an edge of the 'state'; 'se' is shadow of 'e' or SENIL

#	ifdef SHADOW_EDGES
	Count GetTotal_()	{ return state.prev == NIL ? GN(state.n).total : GN(state.prev).stotal; }
#	else
	Count GetTotal_()	{ return GN(state.n).total; }
#	endif

	void Move(PEdge e);
	void MakeLog(PNode stt, PEdge e);		// print into 'logfile' information about current state and transition

//-----------------------------------------------------------------------------
public:
	// CAUTION: the 'data_' array is NOT physically copied, only its pointer.
	// So the data must not change outside this class during lifetime of this object.
	WordGraph(const Symb* data_, int dlen_ = -1, bool insatend_ = true);
	virtual ~WordGraph() { Clear(); }

	bool insatend;		// insert new child at the end of the children list?

	//-------------------------------------------------------------------------
	// information and statistics

	virtual int GetNNodes()		{ return (int)nodes.size(); }
	virtual int GetNEdges()		{ return (int)edges.size(); }
	virtual int GetNFinals()	{ return (int)finals.size(); }
	virtual int GetNSEdges()	{ return (int)sedges.size(); }
	virtual void PrintStat(FILE* f);

	virtual int GetMemUsage();		// real number of bytes used, without wasted space in 'vector'
	virtual int GetMemAlloc();		// total number of bytes used
	void Print(std::ostream& str = std::cout, uint flags = 1);
	void PrintLbl(std::ostream& str, Edge& e);

	//-------------------------------------------------------------------------
	// definitions for PPM

	virtual void TransformForPPM(PPMParam param_ = PPMParam());
	virtual void InitPPM();

	// compression: [str,len_total] -> [len_of_edge,rng,total]
	virtual void Move(Symb* str, int& len, Range& rng, Count& total);

	// decompression: [c,str,len_max] -> [str,len_of_edge,rng]+returned_error
	virtual CprsErr Move(Count c, Symb* str, int& len, Range& rng);
	virtual Count GetTotal()	{ return GetTotal_(); }
};

#endif

