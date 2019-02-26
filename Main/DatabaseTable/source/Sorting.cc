
#ifndef SORT_C
#define SORT_C

#include <RecordComparator.h>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"

using namespace std;


class PairComparator{
private:
	function<bool ()> comparator;
	MyDB_RecordPtr lhs, rhs;

public:
	PairComparator(function<bool ()> comparatorIn,MyDB_RecordPtr l, MyDB_RecordPtr r)
	:comparator(comparatorIn),
	lhs(move(l)),
	rhs(move(r))
	{}

	bool operator () (MyDB_RecordIteratorAltPtr l, MyDB_RecordIteratorAltPtr r) {
		l->getCurrent(lhs);
		r->getCurrent(rhs);
		return !comparator ();
	}

};

void mergeIntoFile (MyDB_TableReaderWriter &sortIntoMe,
					vector <MyDB_RecordIteratorAltPtr> &mergeUs,
					function <bool ()> comparator,
					MyDB_RecordPtr lhs,
					MyDB_RecordPtr rhs)
{
	PairComparator pairComp (comparator,lhs,rhs);
	priority_queue<MyDB_RecordIteratorAltPtr,vector<MyDB_RecordIteratorAltPtr>,PairComparator> minHeap(pairComp);
	// load the first records of all runs into the min heap;
	for(MyDB_RecordIteratorAltPtr recordAltPtr : mergeUs){
		minHeap.push(recordAltPtr);
	}
	MyDB_RecordIteratorAltPtr temp;
	while (!minHeap.empty()){
		// find the smallest record
		temp = minHeap.top();
		minHeap.pop();
		temp->getCurrent(lhs);
		sortIntoMe.append(lhs);
		// if current run has next record, load the alternative iterator into min heap
		if(temp->advance()){
			minHeap.push(temp);
		}
	}
}




vector <MyDB_PageReaderWriter> mergeIntoList (	MyDB_BufferManagerPtr parent,
												MyDB_RecordIteratorAltPtr leftIter,
											  	MyDB_RecordIteratorAltPtr rightIter,
											  	function <bool ()> comparator,
											  	MyDB_RecordPtr lhs,
											  	MyDB_RecordPtr rhs)
{
	MyDB_PageReaderWriter pageRWer(*parent);
	vector<MyDB_PageReaderWriter> list;
	list.push_back(pageRWer);
	leftIter->getCurrent(lhs);
	rightIter->getCurrent(rhs);
	MyDB_RecordPtr small;
	MyDB_RecordIteratorAltPtr nextPtr;
	MyDB_RecordIteratorAltPtr remainPtr;

	// algorithm as merging two sorted linked list
	while (true){
	    // find the smaller record
		if(comparator()){
			small = lhs;
			nextPtr = leftIter;
			remainPtr = rightIter;
		}else{
			small = rhs;
			nextPtr = rightIter;
			remainPtr = leftIter;
		}
		// try to append the smaller record into current page, if it fails, create a new page for appending
		if(!pageRWer.append(small)){
			pageRWer = MyDB_PageReaderWriter(*parent);
			list.push_back(pageRWer);
			pageRWer.append(small);
		}
		if(!nextPtr->advance()){
			break;
		}
		nextPtr->getCurrent(small);
	}

	// one of the iterators of the records has been exhausted, now we exhaust another iterators.
	nextPtr = remainPtr;
	do{
		nextPtr->getCurrent(small);
		if(!pageRWer.append(small)){
			pageRWer = MyDB_PageReaderWriter(*parent);
			list.push_back(pageRWer);
			pageRWer.append(small);
		}
	}while(nextPtr->advance());

	return list;
}




void sort (	int runSize,
			MyDB_TableReaderWriter &sortMe,
			MyDB_TableReaderWriter &sortIntoMe,
		   	function <bool ()> comparator,
		   	MyDB_RecordPtr lhs,
		   	MyDB_RecordPtr rhs)
{
	vector<MyDB_RecordIteratorAltPtr> mergeUs;

	int curPage = 0;
	while (curPage<sortMe.getNumPages()){
		vector<MyDB_PageReaderWriter> pageRWList ;
		pageRWList.push_back(*sortMe[curPage++].sort(comparator,lhs,rhs));
		for(int i=1; i<runSize && curPage<sortMe.getNumPages(); i++,curPage++){
			MyDB_RecordIteratorAltPtr altPtr1 = getIteratorAlt(pageRWList);
			MyDB_PageReaderWriter pageRWer = *sortMe[curPage].sort(comparator,lhs,rhs);
			pageRWList = mergeIntoList(sortMe.getBufferMgr(),altPtr1,pageRWer.getIteratorAlt(),comparator,lhs,rhs);
		}
		mergeUs.push_back(getIteratorAlt(pageRWList));
	}

	mergeIntoFile(sortIntoMe,mergeUs,comparator,lhs,rhs);

}

#endif
