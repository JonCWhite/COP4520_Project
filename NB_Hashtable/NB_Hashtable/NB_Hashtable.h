// Jonathan White, Derek Spooner
// COP 4520 - Spring 2018
// NB_Hashtable.h - Defines NB_Hashtable class.
#pragma once

// Used throughout class to define a type that is the size of a memory word. 
// the hashtable was written on a 64-bit system, so the default is a 64-bit
// addressed type.
#define WORD_SIZE_TYPE uint64_t

#include <algorithm>
#include <atomic>
// required for use of uint64_t
#include <cstdint>

class NB_Hashtable
{
public:
	NB_Hashtable();
	void InitProbeBound(int h);
	int GetProbeBound(int h);
	void ConditionallyRaiseBound(int h, WORD_SIZE_TYPE index);
	void ConditionallyLowerBound(int h, WORD_SIZE_TYPE index);

	int size;
	// bounds must contain values that are thesize of a machine word because it 
	// stores data that will be used to atomically alter addresses
	std::atomic<WORD_SIZE_TYPE>* bounds;
};

// No argument constructor that initializes size to 2^10 (1024). The table size
// is always a power of 2, as certain table operations rely on this assumption.
NB_Hashtable::NB_Hashtable()
{
	size = 1024;
	bounds = new std::atomic<WORD_SIZE_TYPE>[size];
}

// Sets the bound entry bound and scanning bit to <0, false>
void NB_Hashtable::InitProbeBound(int h)
{
	bounds[h] = (WORD_SIZE_TYPE) 0;
}

// Returns the maximum offset of any collision in a probe sequence as well as
// that bound's scanning bit <bound, scanning>
int NB_Hashtable::GetProbeBound(int h)
{
	return bounds[h];
}

// NOTE FOR DEVELOPER (JON): Index has to be WORD_SIZE_TYPE (uint64_t) instead
// of an int because it refers at a memory location AT index in an array.
void NB_Hashtable::ConditionallyRaiseBound(int h, WORD_SIZE_TYPE index)
{
	WORD_SIZE_TYPE old_bound, new_bound;

	do {
		old_bound = bounds[h];
		new_bound = std::max(old_bound, index);
	} while (!std::atomic_compare_exchange_weak(&(bounds[h]), &old_bound, new_bound));
}

void NB_Hashtable::ConditionallyLowerBound(int h, WORD_SIZE_TYPE index)
{
	WORD_SIZE_TYPE bound = bounds[h]/*, expected*/;

	// If status bit is set, unset it
	if ((bound & (WORD_SIZE_TYPE) 1) == ((WORD_SIZE_TYPE) 1))
	{
		std::atomic_compare_exchange_weak(&bounds[h], &bound, (bound & ~((WORD_SIZE_TYPE) 1)));
	}
	
	if (index > 0)
	{
		//expected = index & ~((WORD_SIZE_TYPE)1);
		while (std::atomic_compare_exchange_weak(&bounds[h], &index, (index | (WORD_SIZE_TYPE)1)))
		{
			WORD_SIZE_TYPE i = index - 1;
			while ((i > 0) && (!DoesBucketContainCollision(h, i)))
			{
				i--;
			}
			std::atomic_compare_exchange_weak(&bounds[h], &index, i);
		}
	}
}