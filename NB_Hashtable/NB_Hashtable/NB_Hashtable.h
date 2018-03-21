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
#include <exception>
#include <functional>
#include <iostream>
#include <string>

class NB_Hashtable
{
public:
	NB_Hashtable();
	void Init();
	// "The key and state must be modified atomically; we use the <., .> operator
	// to represent packing them into a single word. A key k is considered
	// inserted if some bucket in the table contains <k, member>."
	// So, although the prototype for Lookup uses Key K, we use WORD_SIZE_TYPE
	// rather than some custom class key.
	bool Lookup(WORD_SIZE_TYPE k);
	bool Insert(WORD_SIZE_TYPE k);

private:
	void InitProbeBound(WORD_SIZE_TYPE h);
	// This returns an index of bounds. It is int in the psuedocode, but bounds
	// contains WORD_SIZE_TYPE in our code. Thus this functions must instead
	// return WORD_SIZE_TYPE.
	WORD_SIZE_TYPE GetProbeBound(WORD_SIZE_TYPE h);
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

// Initializes buckets and bounds arrays.
void NB_Hashtable::Init()
{
	for (int i = 0; i < size; i++)
	{
		InitProbeBound(i);
		buckets[i] = 0;
	}
}

// Determines whether k is a member of the set.
bool NB_Hashtable::Lookup(WORD_SIZE_TYPE k)
{
	WORD_SIZE_TYPE h = std::hash<WORD_SIZE_TYPE>{}(k);
	WORD_SIZE_TYPE max = GetProbeBound(h);

	// i is WORD_SIZE_TYPE so it can be passed to Bucket().
	for (WORD_SIZE_TYPE i = 0; i < max; i++)
	{
		if (*Bucket(h, i) == k)
		{
			return true;
		}
	}

	return false;
}

// Insert k into the set if it is not a member
bool NB_Hashtable::Insert(WORD_SIZE_TYPE k)
{
	WORD_SIZE_TYPE h = std::hash<WORD_SIZE_TYPE>{}(k);
	WORD_SIZE_TYPE i = 0, temp;

	// Attempt to change bucket entry from state empty (00) to busy (01).
	while (!std::atomic_compare_exchange_weak(Bucket(h, i), 0, 1))
	{
		i++;
		if (i >= size)
		{
			try
			{
				throw "Table full";
			}
			catch (std::string s)
			{
				std::cout << s << std::endl;
			}
		}
	}
	// attempt to insert a unique copy of k
	do
	{
		// set state bit of <key, state> to inserting (10)
		temp = (k | (WORD_SIZE_TYPE)2);
		*Bucket(h, i) = temp;
		ConditionallyRaiseBound(h, i);
		
		// Scan through probe sequence
		WORD_SIZE_TYPE max = GetProbeBound(h);
		for (WORD_SIZE_TYPE j = 0; j < max; j++)
		{
			if (j != i)
			{
				// Stall concurrent inserts
				if (*Bucket(h, j) = temp)
				{
					std::atomic_compare_exchange_strong(Bucket(h, j),
						&temp,
						1);
				}
				// Abort if k is already a member
				if (*Bucket(h, j) == (k | (WORD_SIZE_TYPE)3))
				{
					*Bucket(h, i) = 1;
					ConditionallyLowerBound(h, i);
					*Bucket(h, i) = 0;
					return false;
				}
			}
		}
	// attempt to set bit of <key, state> to member (11)
	} while (!std::atomic_compare_exchange_weak(Bucket(h,i),
		&temp, 
		(k | (WORD_SIZE_TYPE) 3)));

	return true;
}




// Private member functions

// Sets the bound entry bound and scanning bit to <0, false>
void NB_Hashtable::InitProbeBound(WORD_SIZE_TYPE h)
{
	bounds[h] = (WORD_SIZE_TYPE) 0;
}

// Returns the maximum offset of any collision in a probe sequence as well as
// that bound's scanning bit <bound, scanning>
WORD_SIZE_TYPE NB_Hashtable::GetProbeBound(WORD_SIZE_TYPE h)
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

// Return bucket entry at hash value plus offset (using quadratic probing)
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