#ifndef COPLUS_ARENA_ARENA_H_
#define COPLUS_ARENA_ARENA_H_

#include <cstring> // memset
#include <cstdint>
#include <bitset>
#include <iostream>
#include <new> // bad_alloc

#include "coplus/arena/arena_port.h"
#include "coplus/util.h"

namespace coplus {

namespace arena {

// arena options //
// user defined
constexpr int32_t kPageTotalSize = 1024 * 16 * 11; // kPageTotalSize * kPageLen == The Maximum Space In Total You Can Preserve
constexpr int32_t kSpanSlotSize = 60; // kSpanSlotSize * kPageLen == The Maximum Chunk You Can Malloc
// suggested setting for larger request:
// [1024 * 16 * 16, 128] when TestSize is 12000
// [1024 * 16 * 22, 128] when TestSize is 15000


// internal data type //
// all bigger than 0 !
// all 4 byte per unit ! // ERROR for oFree
typedef int16_t SpanClass;
typedef int16_t PageClass;
typedef int32_t PageHandle;
typedef int16_t PageOffset;
constexpr int32_t kSpanClassSize = 2;
constexpr int32_t kPageClassSize = 2;
constexpr int32_t kPageHandleSize = 2+2;
constexpr int32_t kPageOffsetSize = 4;
// alignment
constexpr int32_t kSpanHeaderSize = 6+4+2; // 16 * 3
constexpr int32_t kPageHeaderSize = 10+4+2; // 16 * 5
constexpr int32_t kFlagSize = 4;
#pragma pack(1)
// alignment
typedef int32_t MinHeader;
struct SpanHeader{
	MinHeader Flag; // size of concated pages
	PageHandle hPri;
	PageHandle hNext; // -1 for occupied, 0 for end
};
struct PageHeader{
	MinHeader Flag; // negative, bin slot, not object size
	PageHandle hPri;
	PageHandle hNext; // -1 for occupied
	PageOffset oFree; // offset in current page
	int16_t nUsed; // size of available objects
};
typedef SpanHeader LinkedNodeHeader;
#pragma pack()

// pre-designed
constexpr int32_t kPageLen = 4*1024; // 4 KB per page
constexpr int32_t kPageLenWid = 12; // Bit Width of kPageLen
constexpr int32_t kTotalSizeByte = kPageTotalSize * kPageLen;
constexpr int32_t kMemoryCapacity = kPageLen * kPageTotalSize;
constexpr int32_t kBinSlotSize = 39;
constexpr int32_t kMaxObjectAvailable = 4080;
constexpr int32_t kMaxChunkAvailable = kSpanSlotSize; // 1024 KB at most at one time
constexpr int kBinSlotTemplate[kBinSlotSize+1] = \
	{ 0, 4, 8, 16, 32, 48, 56, 64, 72, 80, 88, 96, 112, 128, 144, 160, 176, 192, \
		208, 224, 240, 256, 272, 288, 320, 352, 384, 448, 512, 576, 672, 800, \
		1024, 1088, 1344, 1632, 2016, 2720, 3040, 4080                       };
		//    2           2           2     3     3
constexpr int kSelfReservedPage = ((kPageTotalSize+1)/8+kPageHandleSize*(kBinSlotSize + kSpanSlotSize + 2)) / kPageLen + 1;

// global memory heap pointer //
char* kMemoryAllocStart = nullptr;
char* kMemoryReservedStart = nullptr;
char* kMemoryAllocEnd = nullptr;
int32_t kHeapAllocPage = 0;

// global map helper //
// ERROR
#define PtrOf(hPage) 							(kMemoryAllocStart + ((hPage+kSelfReservedPage-1) << kPageLenWid))
// ERROR
#define SpanSlotOf(nSize) 				(((nSize + kSpanHeaderSize-1) >> kPageLenWid) + 1)
// ERROR
#define PageHandleOf(ptr) 				(((ptr - kMemoryAllocStart) >> kPageLenWid) - kSelfReservedPage + 1)

// linked page helper //
#define LinkedNodeMerge(pri,next) 		do{\
																			(pri>0) && (reinterpret_cast<LinkedNodeHeader*>(PtrOf(pri))->hNext=next);\
																			(next>0) &&	(reinterpret_cast<LinkedNodeHeader*>(PtrOf(next))->hPri=pri);  }while(0)
#define LinkedNodeDelete(node)				LinkedNodeMerge(node->hPri,node->hNext)
// ERROR																			
#define LinkedNodeDeleteHead(node) 		do{\
																			(node->hNext > 0) && memset(PtrOf(node->hNext) + kFlagSize, 0, kPageHandleSize);}while(0)


#ifdef _ARENA_DEBUG_
#define msg(m)						do{::std::cout << __func__ << ": " << #m << std::endl;}while(0)	
#define varplot(v)				do{::std::cout << __func__ << ": " << #v << " = " << (v) << std::endl;}while(0)
#define arrplot(arr,size) do{std::cout << #arr << ": " ;for(int arrplot_i = 0 ; arrplot_i < size; arrplot_i++)std::cout << arr[arrplot_i] << " ";std::cout << std::endl;}while(0)
#define arrplots(arr,size) do{std::cout << #arr << ": " ;for(int arrplot_i = 0 ; arrplot_i < size; arrplot_i++)std::cout << arr[arrplot_i] << "-" << reinterpret_cast<SpanHeader*>(PtrOf(arr[arrplot_i]))->Flag << "->" << reinterpret_cast<SpanHeader*>(PtrOf(arr[arrplot_i]))->hNext << " ";std::cout << std::endl;}while(0)
#else
#define msg(m) 						do{}while(0)
#define varplot(v) 				do{}while(0)
#define arrplot(arr,size) do{}while(0)
#define arrplots(arr,size) do{}while(0)
#endif

// #define BinSpanSize(x)		( (x <= 0 || x > kBinSlotSize)&&(std::cout << __LINE__<<":"<<x<<std::endl), BinSpanSizeInner(x))

class Arena: public NoCopy{
	// store in first reserved page 4 KB at most
	::std::bitset<kPageTotalSize+1>* is_head_; // 256 byte
	PageHandle* bin_; // 2 * (128+2)
	PageHandle* span_list_; // 2 * (38+1)
 public:
 	Arena():is_head_(nullptr),bin_(nullptr),span_list_(nullptr){ 
		if (!Init()){
			throw std::bad_alloc();
		}
 	}
 	~Arena(){
 		FreeHeap(kMemoryAllocStart, kMemoryAllocEnd-kMemoryAllocStart);
 		kMemoryAllocStart = nullptr;
 		kMemoryAllocEnd = nullptr;
 		kMemoryAllocStart = nullptr;
 		kHeapAllocPage = 0;
 	#ifdef _ARENA_DEBUG_
 		varplot(deMallocSmallChunk);
 		varplot(tMallocSmallChunk);
 		varplot(deMallocSmallChunkSpan);
 		varplot(tMallocSmallChunkSpan);
 		varplot(deMallocSmallChunkSplitting);
 		varplot(tMallocSmallChunkSplitting);
 		varplot(deMallocBigChunk);
 		varplot(tMallocBigChunk);
 		varplot(deMallocBigChunkSplitting);
 		varplot(tMallocBigChunkSplitting);
 		varplot(deFreeObject);
 		varplot(tFreeObject);
 		varplot(deFreeMergeSpan);
 		varplot(tFreeMergeSpan);
 	#endif
 	}
 	char* Malloc(int32_t size){
 	#ifdef _ARENA_DEBUG_
 		// varplot(size);
 		__arena_timer.Start();
 	#endif
		if(size <= 0)return nullptr;
		register PageHandle hPage;
		register char* pageData;
		// big chunk
		if( unlikely(size > kMaxObjectAvailable) ){
			SpanHeader* spanHeader;
			SpanClass spanSlot = SpanSlotOf(size);
			hPage = span_list_[spanSlot];
			if(hPage > 0){ // have suitable free span
				// update free list here //
				COPLUS_ASSERT((*is_head_)[hPage]);
				pageData = PtrOf(hPage);
				spanHeader = reinterpret_cast<SpanHeader*>(pageData);
				COPLUS_ASSERT(spanHeader->Flag == spanSlot);
				span_list_[spanSlot] = spanHeader->hNext; // free head point to next
				// ERROR revisit
				// spanHeader->hNext = -1;
				// LinkedNodeDeleteHead(spanHeader);
				LinkedNodeDeleteHead(spanHeader);
				spanHeader->hNext = -1;
			#ifdef _ARENA_DEBUG_
				tMallocBigChunk += __arena_timer.End();
				++deMallocBigChunk;
			#endif
				return pageData + kSpanHeaderSize;	
			}
			// no available span this size
			// split span here //
			SpanClass bigSpanSlot;
			// visit all bigger span until find free or outOfBounds
			for(bigSpanSlot=spanSlot+1; (span_list_[bigSpanSlot]<=0) && (bigSpanSlot <= kSpanSlotSize); bigSpanSlot++);
			// if outOfBounds, allocate from heap
			if(bigSpanSlot>kSpanSlotSize)(AllocMaxChunkFromHeap() && (--bigSpanSlot));
			COPLUS_ASSERT(bigSpanSlot <= kSpanSlotSize);
			// if alloc fail	
			if(unlikely(span_list_[bigSpanSlot] <= 0))return nullptr;
			hPage = span_list_[bigSpanSlot];
			COPLUS_ASSERT((*is_head_)[hPage]);
			// process splited span here //
			pageData = PtrOf(hPage);
			spanHeader = reinterpret_cast<SpanHeader*>(pageData);
			// maintain bigger slot
			span_list_[bigSpanSlot] = spanHeader->hNext;	
			LinkedNodeDeleteHead(spanHeader);
			// maintain slot that fit
			spanHeader->Flag = spanSlot;
			spanHeader->hNext = -1;
			// maintain the rest span
			SpanClass anotherSpan = bigSpanSlot - spanSlot;
			is_head_->set(hPage + spanSlot); // set as head
			spanHeader = reinterpret_cast<SpanHeader*>(PtrOf(hPage + spanSlot));
			spanHeader->Flag = anotherSpan;
			spanHeader->hPri = 0;
			spanHeader->hNext = span_list_[anotherSpan];
			(spanHeader->hNext>0) && (reinterpret_cast<LinkedNodeHeader*>(PtrOf(spanHeader->hNext))->hPri = hPage + spanSlot);
			span_list_[anotherSpan] = hPage + spanSlot;
		#ifdef _ARENA_DEBUG_
			tMallocBigChunkSplitting += __arena_timer.End();
			++deMallocBigChunkSplitting;
		#endif
			return pageData + kSpanHeaderSize;
		}
		// small chunk
		PageClass binSlot = BinSlot(size);
		int usedSpanSize = BinSpanSize(binSlot);
		hPage = bin_[binSlot];
		PageHeader* pageHeader = nullptr;
		// two scope at most
		// ERROR
		(hPage>0) && (pageHeader=reinterpret_cast<PageHeader*>(PtrOf(hPage))) && (pageHeader->oFree<=1) \
		 && (hPage = pageHeader->hNext) && (pageHeader = reinterpret_cast<PageHeader*>(PtrOf(hPage)));
		 // && (hPage = pageHeader->hNext)>0 && (pageHeader = reinterpret_cast<PageHeader*>(PtrOf(hPage))) && (pageHeader->oFree<=1)\

		// while( (hPage > 0) && (pageHeader=reinterpret_cast<PageHeader*>(PtrOf(hPage))) && (pageHeader->oFree<=1) ){
		// 	hPage = pageHeader->hNext;
		// 	break;
		// }
		
		if( likely((hPage > 0) && (pageHeader->oFree > 1) ) ) { // find free obj
			COPLUS_ASSERT((*is_head_)[hPage]);
			// add reference here //
			pageData = PtrOf(hPage);
			pageHeader->nUsed ++;  
			char* freeObject = pageData + (pageHeader->oFree); // locate the object

			// not COPLUS_ASSERT(Chunk == 0)
			if(((*reinterpret_cast<PageOffset*>(freeObject)) == 0) &&
				(freeObject + (kBinSlotTemplate[binSlot]<<1) <= pageData + (usedSpanSize<<kPageLenWid)) ){ // meaning not initialized
				pageHeader->oFree += (kBinSlotTemplate[binSlot]); // add len of a object
				memset(freeObject + kBinSlotTemplate[binSlot], 0, kPageOffsetSize); // forward memset
			}
			else{
				pageHeader->oFree = (*reinterpret_cast<PageOffset*>(freeObject)); // store in truncated offset
			}
		#ifdef _ARENA_DEBUG_
			tMallocSmallChunk += __arena_timer.End();
			++deMallocSmallChunk;
		#endif
			return freeObject;
		}
		else{ // no free obj, looking for free span
			hPage = span_list_[usedSpanSize];
			if(hPage > 0){ // have free span this size
				// [Timing Profile] 0.0015 ms //
				// update free list here //
				COPLUS_ASSERT((*is_head_)[hPage]);
				pageData = PtrOf(hPage);
				SpanHeader* spanHeader = reinterpret_cast<SpanHeader*>(pageData);
				// delete from big span slot
				span_list_[usedSpanSize] = spanHeader->hNext; 
				(spanHeader->hNext > 0) && memset(PtrOf(spanHeader->hNext) + kFlagSize, 0, kPageHandleSize);
 				// turn to page
				pageHeader = reinterpret_cast<PageHeader*>(pageData);
				COPLUS_ASSERT(spanHeader->Flag == usedSpanSize);
				pageHeader->Flag = -binSlot; 
				pageHeader->nUsed = 1; // number of used object
				pageHeader->hPri = 0; // as head
				pageHeader->hNext = bin_[binSlot];

				bin_[binSlot] = hPage;
				(pageHeader->hNext > 0) && (reinterpret_cast<LinkedNodeHeader*>(PtrOf(pageHeader->hNext))->hPri = hPage);
				// not COPLUS_ASSERT(Chunk==0)
				pageHeader->oFree = (kPageHeaderSize + kBinSlotTemplate[binSlot]); // set to next
				memset(pageData+kPageHeaderSize+kBinSlotTemplate[binSlot], 0, kPageOffsetSize); // forward memset
			#ifdef _ARENA_DEBUG_
				tMallocSmallChunkSpan += __arena_timer.End();
				++deMallocSmallChunkSpan;
			#endif
				return pageData + kPageHeaderSize;
			}
			// no available span this size
			// split span here //
			SpanClass bigSpanSlot;
			for(bigSpanSlot = usedSpanSize+1; (span_list_[bigSpanSlot]<=0) && (bigSpanSlot <= kSpanSlotSize); bigSpanSlot++);
			(bigSpanSlot>kSpanSlotSize) && AllocMaxChunkFromHeap() && (--bigSpanSlot);
			COPLUS_ASSERT(bigSpanSlot <= kSpanSlotSize);
			if(unlikely(span_list_[bigSpanSlot] <= 0))return nullptr;
			// find bigger span to split
			hPage = span_list_[bigSpanSlot];
			pageData = PtrOf(hPage);
			SpanHeader* spanHeader = reinterpret_cast<SpanHeader*>(pageData);

			// delete from big slot
			span_list_[bigSpanSlot] = spanHeader->hNext;
			(spanHeader->hNext > 0) && memset(PtrOf(spanHeader->hNext) + kFlagSize, 0, kPageHandleSize); // nextPage->pri=0 // NOTICE
			// maintain the rest
			SpanClass anotherSpan = bigSpanSlot - usedSpanSize;		
			is_head_->set(hPage + usedSpanSize);

			spanHeader = (SpanHeader*)(PtrOf(hPage + usedSpanSize));
			spanHeader->Flag = anotherSpan;
			spanHeader->hPri = 0;	
			spanHeader->hNext = span_list_[anotherSpan];			
			(spanHeader->hNext>0) && (((LinkedNodeHeader*)(PtrOf(span_list_[anotherSpan])))->hPri = hPage + usedSpanSize);
			span_list_[anotherSpan] = hPage + usedSpanSize;
			// now process returned span
			pageHeader = reinterpret_cast<PageHeader*>(pageData);
			pageHeader->Flag = -binSlot; // slot
			pageHeader->nUsed = 1;
			pageHeader->hPri = 0; // as head
			pageHeader->hNext = bin_[binSlot];
			bin_[binSlot] = hPage;
			(pageHeader->hNext > 0) && (reinterpret_cast<LinkedNodeHeader*>(PtrOf(pageHeader->hNext))->hPri = hPage);
			// not COPLUS_ASSERT(pageData==0)
			pageHeader->oFree = ((kPageHeaderSize + kBinSlotTemplate[binSlot])); // set to next
			memset(pageData+kPageHeaderSize+kBinSlotTemplate[binSlot], 0, kPageOffsetSize); // memset next
		#ifdef _ARENA_DEBUG_
			tMallocSmallChunkSplitting += __arena_timer.End();
			++deMallocSmallChunkSplitting;
		#endif
			return pageData + kPageHeaderSize;
		}
	}
	bool Plot(void){
		// for(int i = 1; i < kHeapAllocPage; i++){
		// 	std::cout << (*is_head_)[i];
		// }
		// std::cout << std::endl;
		arrplots(bin_, kBinSlotSize+1);
		arrplots(span_list_, kSpanSlotSize+1);
		return true;
	}
	bool Free(char* ptr){
	#ifdef _ARENA_DEBUG_
		__arena_timer.Start();
	#endif
		PageHandle hPage = SpanHandleOf(ptr);
		char* pagePtr = PtrOf(hPage);
		MinHeader* header = reinterpret_cast<MinHeader*>(pagePtr);
		if((*header) <= 0){ // small object
			PageHeader* pageHeader = reinterpret_cast<PageHeader*>(pagePtr);

			// ERROR
			// || bin_[-(*header)] == hPage
			if( (--pageHeader->nUsed) > 0 || pageHeader->oFree < ((BinSpanSize(-*header))<<(kPageLenWid-3)) || bin_[-(*header)] == hPage){ 
				// not empty or (NOTICE) address too low (<0.25 usedSpanSize)
				// flush here
				*(reinterpret_cast<PageOffset*>(ptr)) = pageHeader->oFree;
				pageHeader->oFree = (ptr - pagePtr);				
				if( ((PageHeader*)PtrOf(bin_[-(*header)]))->nUsed > (pageHeader->nUsed)  ){
					((PageHeader*)PtrOf(bin_[-(*header)]))->hPri = hPage;
					((PageHeader*)PtrOf(bin_[-(*header)]))->hNext = pageHeader->hNext;
					((PageHeader*)PtrOf(pageHeader->hNext))->hPri = pageHeader->hNext;
					pageHeader->hNext = bin_[-(*header)];
					pageHeader->hPri = 0;
					bin_[-(*header)] = hPage;
				}
			#ifdef _ARENA_DEBUG_
				tFreeObject += __arena_timer.End();
				++deFreeObject;
			#endif
				return true;
			}
			// free this page here //
			if(pageHeader->hPri == 0){ // first page in bin_
				bin_[-pageHeader->Flag] = pageHeader->hNext;
				LinkedNodeDeleteHead(pageHeader);
			}
			else{
				LinkedNodeDelete(pageHeader);
			}
			(*header) = BinSpanSize(-pageHeader->Flag); // prepare for span merge
		}
		SpanHeader* spanHeader = (SpanHeader*)pagePtr;

		// Lazy Strategy
		// || (*header) >= 8
		if( span_list_[(*header)] <= 0 || (*header) >= 12){
			// MUST add this one if condition is varified
			if(span_list_[(*header)] > 0)reinterpret_cast<LinkedNodeHeader*>(PtrOf(span_list_[(*header)]))->hPri = hPage;
			spanHeader->hNext = span_list_[(*header)];
			// ProtectNext(spanHeader);
			spanHeader->hPri = 0;
			// is_head_->set(hPage);
			span_list_[(*header)] = hPage;
			return true;
		}
		// scoping back-forth to merge //
		is_head_->reset(hPage); // ERROR
		PageHandle hNeighbor = hPage + (*header);
		int maxToMerge = kSpanSlotSize - (*header); // if big, meaning current small
		if(hNeighbor <= kHeapAllocPage - kSelfReservedPage ){ // forward // ERROR // ERROR
			spanHeader = reinterpret_cast<SpanHeader*>(PtrOf(hNeighbor));
			// delete neighbor
			if((spanHeader->hNext >= 0) && (spanHeader->Flag > 0) && (spanHeader->Flag <= maxToMerge)){ // -1 for occupied
				if(spanHeader->hPri==0){
					span_list_[spanHeader->Flag]=spanHeader->hNext;
					LinkedNodeDeleteHead(spanHeader);
				}
				else{
					LinkedNodeDelete(spanHeader);
				}
				is_head_->reset(hNeighbor);
				(*header) += spanHeader->Flag; // size add
				maxToMerge -= spanHeader->Flag;
			}
		}
		hNeighbor = SpanHandleOf(hPage-1);
		// && maxToMerge > (kSpanSlotSize >> 2)
		if(hNeighbor > 0 && span_list_[*header] > 0 && (*header) < 12){
			spanHeader = reinterpret_cast<SpanHeader*>(PtrOf(hNeighbor));
			// delete neighbor
			if((spanHeader->hNext >= 0) && (spanHeader->Flag > 0) && (spanHeader->Flag <= maxToMerge)){
				if(spanHeader->hPri==0){
					span_list_[spanHeader->Flag]=spanHeader->hNext;
					LinkedNodeDeleteHead(spanHeader);
				}
				else{
					LinkedNodeDelete(spanHeader);
				}
				is_head_->reset(hNeighbor);
				spanHeader->Flag += (*header);
				hPage = hNeighbor;
				pagePtr = PtrOf(hNeighbor);
			}
		}
		// pagePtr: header of this chunk, hPage: handle of this chunk
		// not memset
		// memset(pagePtr+2, 0, spanHeader->Flag * kPageLen - 2);
		spanHeader = reinterpret_cast<SpanHeader*>(pagePtr);
		// ERROR
		COPLUS_ASSERT(spanHeader->Flag > 0);
		is_head_->set(hPage);
		if(span_list_[spanHeader->Flag] > 0)reinterpret_cast<LinkedNodeHeader*>(PtrOf(span_list_[spanHeader->Flag]))->hPri = hPage;
		spanHeader->hNext = span_list_[spanHeader->Flag];
		spanHeader->hPri = 0;
		span_list_[spanHeader->Flag] = hPage;
		if(spanHeader->hNext>0)
			reinterpret_cast<LinkedNodeHeader*>(PtrOf(spanHeader->hNext))->hPri = hPage;
	#ifdef _ARENA_DEBUG_
		tFreeMergeSpan += __arena_timer.End();
		++deFreeMergeSpan;
	#endif
		return true;
	}
 private:
	inline bool Init(void){
		if(kMemoryAllocStart)return false; // already inited
		// initial heap space
		// std::cout << "kMemoryCapacity = " << kMemoryCapacity << std::endl;
		kMemoryAllocStart = GetReserved(kMemoryCapacity);
		if(kMemoryAllocStart == nullptr || kMemoryAllocStart == NULL)throw std::bad_alloc();
		kMemoryAllocEnd = kMemoryAllocStart + kMemoryCapacity;
		kMemoryReservedStart = kMemoryAllocStart;
		// ERROR here
		AllocMemFromHeap(kPageLen * kSelfReservedPage);
		// construct meta data
		bin_ = new(kMemoryAllocStart)\
		 PageHandle[kBinSlotSize+1]();
		span_list_ = new(kMemoryAllocStart + kPageHandleSize * (kBinSlotSize+1))\
		 PageHandle[kSpanSlotSize+1]();
		is_head_ = new(kMemoryAllocStart + kPageHandleSize * (kBinSlotSize+1) + kPageHandleSize * (kSpanSlotSize+1))\
		 ::std::bitset<kPageTotalSize+1>();
		// build up initial span
		for(int i = 0; i < 128; i++)COPLUS_ASSERT(AllocArbitaryChunkFromHeap(2));
		// for(int i = 3; i < kMaxChunkAvailable; i++)COPLUS_ASSERT(AllocArbitaryChunkFromHeap(i));
		for(int i = 0; i < 1024; i++)COPLUS_ASSERT(AllocMaxChunkFromHeap());
		return true;
	}
	// Heap allocation //
	inline static char* AllocMemFromHeap(uint32_t size){ // byte
		if( (kHeapAllocPage +( size>>kPageLenWid)>kPageTotalSize) || (kMemoryReservedStart + size  > kMemoryAllocEnd) ){
			throw std::bad_alloc();
			return nullptr;
		}
		if(!Commit(kMemoryReservedStart,size )){
			throw std::bad_alloc();
			return nullptr;
		}
		kHeapAllocPage += (size>>kPageLenWid);
		return (kMemoryReservedStart+=size)-(size);
	}
	inline static bool DeallocMemToHeap(uint32_t size){ // byte
		if(kMemoryReservedStart - size < kMemoryAllocStart)return false;
		bool ret = Decommit(kMemoryReservedStart - size, size);
		if(ret)kMemoryReservedStart -= (size);
		return ret;
	}
	inline void Vaccum(void){
		return ; // not implemented yet
	}
	inline bool AllocMaxChunkFromHeap(void){
		char* newSpan = AllocMemFromHeap(kMaxChunkAvailable << kPageLenWid);
		if(!newSpan)return false;
		PageHandle hNewSpan = PageHandleOf(newSpan);
		is_head_->set(hNewSpan);
		SpanHeader* header = reinterpret_cast<SpanHeader*>(newSpan);
		header->Flag = kSpanSlotSize;
		header->hNext = span_list_[kSpanSlotSize];
		if(span_list_[kSpanSlotSize]>0)((SpanHeader*)PtrOf(span_list_[kSpanSlotSize]))->hPri = hNewSpan;
		span_list_[kSpanSlotSize] = hNewSpan; // first page
		header->hPri = 0;
		return true;
	}
	inline bool AllocArbitaryChunkFromHeap(size_t page){
		char* newSpan = AllocMemFromHeap(page << kPageLenWid);
		if(!newSpan)return false;
		PageHandle hNewSpan = PageHandleOf(newSpan);
		is_head_->set(hNewSpan);
		SpanHeader* header = reinterpret_cast<SpanHeader*>(newSpan);
		header->Flag = page;
		header->hNext = span_list_[page];
		if(span_list_[page]>0)((SpanHeader*)PtrOf(span_list_[page]))->hPri = hNewSpan;
		span_list_[page] = hNewSpan; // first page
		header->hPri = 0;
		return true;		
	}
	inline PageHandle SpanHandleOf(char* ptr){
		PageHandle cur = PageHandleOf(ptr);
		while((cur > 0) && !(*is_head_)[cur] )--cur;
		return cur;
	}
	inline PageHandle SpanHandleOf(PageHandle hPage){
		while((hPage > 0) && !(*is_head_)[hPage])--hPage;
		return hPage;
	}
	inline static int BinSpanSize(int BinSlot){
		COPLUS_ASSERT(BinSlot > 0 && BinSlot <= 39);
		if(BinSlot > 37)return 6; // 38, 39
		// if(BinSlot > 32 && (BinSlot & 0x01 == 1))return 2; // 33, 35, 37
		if(BinSlot >= 32)return 4;
		return 2;
	}
	// inline static int BinSlot(int size){
	// 	COPLUS_ASSERT(size <= 4080);
	// 	int lo=0,mid,hi=kBinSlotSize;
	// 	while(hi > lo+1){
	// 		mid = lo + ((hi-lo)>>1);
	// 		int bin = kBinSlotTemplate[mid];
	// 		if(bin == size)return mid;
	// 		else if(bin < size)lo = mid;
	// 		else hi = mid;
	// 	}
	// 	return hi;
	// }
	inline static int BinSlot(int size){
		COPLUS_ASSERT(size <= 4080);
		if(size <= 48){
			if(size <= 4)return 1;
			if(size <= 8)return 2;
			if(size <= 16)return 3;
			if(size <= 32)return 4;
			return 5;
		}
		else if(size <= 96){
			return ((size-1) >> 3);
		}
		else if(size <= 288){ // 23
			return ((size-1) >> 4) + 6;
		}
		int lo=24,mid,hi=kBinSlotSize;
		while(hi > lo+1){
			mid = lo + ((hi-lo)>>1);
			int bin = kBinSlotTemplate[mid];
			if(bin == size)return mid;
			else if(bin < size)lo = mid;
			else hi = mid;
		}
		return hi;
	}
}; // class Arena

} // namespace arena


}

#endif // COPLUS_ARENA_ARENA_H_