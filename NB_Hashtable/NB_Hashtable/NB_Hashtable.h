// Jonathan White, Derek Spooner
// COP 4520 - Spring 2018
// NB_Hashtable.h - Defines NB_Hashtable class.
#pragma once

// NOTE FOR DEVELOPER (JON): This code is currently based off of the pseudocode
// in figure 6, which has some issues discussed in the paper. If necessary for
// the project, the code will need to be updated to include a version counter
// as seen in figure 8. This second version is designed to circumvent the ABA
// problem.

// Used throughout class to define a type that is the size of a memory word. 
// the hashtable was written on a 64-bit system, so the default is a 64-bit
// addressed type.
#define WORD_SIZE_TYPE uint64_t

#include <algorithm>
#include <atomic>
// required for use of uint64_t
#include <cstdint>
#include <functional>

class NB_Hashtable
{
public:
	NB_Hashtable();
	void Init();

private:
	void InitProbeBound(WORD_SIZE_TYPE h);
	int GetProbeBound(WORD_SIZE_TYPE h);
	void ConditionallyRaiseBound(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index);
	void ConditionallyLowerBound(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index);
	std::atomic<WORD_SIZE_TYPE>* Bucket(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index);
	bool DoesBucketContainCollisions(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index);

	int size;
	// bounds must contain values that are thesize of a machine word because it 
	// stores data that will be used to atomically alter addresses
	std::atomic<WORD_SIZE_TYPE>* bounds;
	std::atomic<WORD_SIZE_TYPE>* buckets;
};

// Public member functions

// No argument constructor that initializes size to 2^10 (1024). The table size
// is always a power of 2, as certain table operations rely on this assumption.
NB_Hashtable::NB_Hashtable()
{
	size = 1024;
	bounds = new std::atomic<WORD_SIZE_TYPE>[size];
	buckets = new std::atomic<WORD_SIZE_TYPE>[size];
}

void NB_Hashtable::Init()
{
	for (int i = 0; i < size; i++)
	{
		InitProbeBound(i);
		buckets[i] = 0;
	}
}




// Private member functions

// Sets the bound entry bound and scanning bit to <0, false>
void NB_Hashtable::InitProbeBound(WORD_SIZE_TYPE h)
{
	bounds[h] = (WORD_SIZE_TYPE) 0;
}

// Returns the maximum offset of any collision in a probe sequence as well as
// that bound's scanning bit <bound, scanning>
int NB_Hashtable::GetProbeBound(WORD_SIZE_TYPE h)
{
	return bounds[h];
}

// NOTE FOR DEVELOPER (JON): Index has to be WORD_SIZE_TYPE (uint64_t) instead
// of an int because it refers at a memory location AT index in an array.
// Ensure maximum >= index
void NB_Hashtable::ConditionallyRaiseBound(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index)
{
	WORD_SIZE_TYPE old_bound, new_bound;

	do {
		old_bound = bounds[h];
		new_bound = std::max(old_bound, index);
	} while (!std::atomic_compare_exchange_weak(&(bounds[h]), &old_bound, new_bound));
}

// Allow maximum < index
void NB_Hashtable::ConditionallyLowerBound(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index)
{
	WORD_SIZE_TYPE bound = bounds[h]/*, expected*/;

	// If status bit is set, unset it
	if ((bound & (WORD_SIZE_TYPE) 1) == ((WORD_SIZE_TYPE) 1))
	{
		std::atomic_compare_exchange_weak(&bounds[h], &bound, (bound & ~((WORD_SIZE_TYPE) 1)));
	}
	
	// 
	if (index > 0)
	{
		//expected = index & ~((WORD_SIZE_TYPE)1);
		while (std::atomic_compare_exchange_weak(&bounds[h], &index, (index | (WORD_SIZE_TYPE)1)))
		{
			WORD_SIZE_TYPE i = index - 1;
			while ((i > 0) && (!DoesBucketContainCollisions(h, i)))
			{
				i--;
			}
			std::atomic_compare_exchange_weak(&bounds[h], &index, i);
		}
	}
}

std::atomic<WORD_SIZE_TYPE>* NB_Hashtable::Bucket(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index)
{
	return &buckets[(h + index * (index + 1) / 2) % size];
}

// In the paper, - refers to the empty state in <key, state>. There are 4
// states: empty, busy, inserting, and member. The paper was written in 2005
// and more than on a 32-bit system. This means that there are 2 unused bits
// at the end of every address---just enough to identify these 4 states. 
// Tentatively, here the codes I am using for my state codes:
// empty = 00
// busy = 01
// inserting = 10
// member = 11
bool NB_Hashtable::DoesBucketContainCollisions(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index)
{
	WORD_SIZE_TYPE k= *Bucket(h, index);
	return ((k != 0) && (std::hash<WORD_SIZE_TYPE>{}(k) == h));
}