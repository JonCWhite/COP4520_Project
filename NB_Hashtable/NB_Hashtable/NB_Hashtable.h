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

// Constants for <state, key>
// The order of the key and state was swapped so it would be easier to recover
// the key. A key is always assumed to have its upper two bits unset.
#define EMPTY 0x0000000000000000
#define BUSY 0x4000000000000000
#define INSERTING 0x8000000000000000
#define MEMBER 0xC000000000000000

// Constants for <scanning bit, bound>
#define SCAN_TRUE 0x8000000000000000
#define SCAN_FALSE 0x0000000000000000

// Libraries used for testing
#include <bitset>

#include <algorithm>
#include <atomic>
// required for use of uint64_t
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <string>

struct BucketT {
	WORD_SIZE_TYPE vs; // <version, state>
	WORD_SIZE_TYPE key;
};

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
	bool Erase(WORD_SIZE_TYPE k);

	// TODO: Test functions?
	//void printbuckets();

private:
	void InitProbeBound(WORD_SIZE_TYPE h);
	// This returns an index of bounds. It is int in the psuedocode, but bounds
	// contains WORD_SIZE_TYPE in our code. Thus this functions must instead
	// return WORD_SIZE_TYPE.
	WORD_SIZE_TYPE GetProbeBound(WORD_SIZE_TYPE h);
	void ConditionallyRaiseBound(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index);
	void ConditionallyLowerBound(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index);
	std::atomic<BucketT>* Bucket(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index);
	bool DoesBucketContainCollisions(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index);

	int size;
	// bounds must contain values that are thesize of a machine word because it 
	// stores data that will be used to atomically alter addresses
	std::atomic<WORD_SIZE_TYPE>* bounds;
	std::atomic<BucketT>* buckets;
};

// Public member functions

// No argument constructor that initializes size to 2^10 (1024). The table size
// is always a power of 2, as certain table operations rely on this assumption.
NB_Hashtable::NB_Hashtable()
{
	size = 1024;
	bounds = new std::atomic<WORD_SIZE_TYPE>[size];
	buckets = new std::atomic<BucketT>[size];
}

// Initializes buckets and bounds arrays.
void NB_Hashtable::Init()
{
	for (int i = 0; i < size; i++)
	{
		InitProbeBound(i);
		BucketT expected = buckets[i].load();
		BucketT desired = expected;
		desired.vs = EMPTY;
		buckets[i].compare_exchange_strong(expected, desired);
	}
}

// Determines whether k is a member of the set.
bool NB_Hashtable::Lookup(WORD_SIZE_TYPE k)
{
	WORD_SIZE_TYPE h = std::hash<WORD_SIZE_TYPE>{}(k);
	WORD_SIZE_TYPE max = GetProbeBound(h);
	WORD_SIZE_TYPE temp = (k | MEMBER);

	// i is WORD_SIZE_TYPE so it can be passed to Bucket().
	for (WORD_SIZE_TYPE i = 0; i <= max; i++)
	{
		// std::cout << (*Bucket(h, i) & ~MEMBER) << std::endl;
		if (*Bucket(h, i) == temp)
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
	temp = EMPTY;
	while (!std::atomic_compare_exchange_weak(Bucket(h, i), &temp, BUSY))
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
		temp = (k | INSERTING);
		*Bucket(h, i) = temp;
		ConditionallyRaiseBound(h, i);
		
		// Scan through probe sequence
		WORD_SIZE_TYPE max = GetProbeBound(h);
		for (WORD_SIZE_TYPE j = 0; j < max; j++)
		{
			if (j != i)
			{
				// Stall concurrent inserts
				if (*Bucket(h, j) == temp)
				{
					std::atomic_compare_exchange_strong(Bucket(h, j),
						&temp,
						BUSY);
				}
				// Abort if k is already a member
				if (*Bucket(h, j) == (k | MEMBER))
				{
					*Bucket(h, i) = BUSY;
					ConditionallyLowerBound(h, i);
					*Bucket(h, i) = EMPTY;
					return false;
				}
			}
		}
	// attempt to set bit of <key, state> to member (11)
	} while (!std::atomic_compare_exchange_weak(Bucket(h,i),
		&temp, 
		(k | MEMBER)));

	// std::bitset<64> tempBits((k | MEMBER));
	// std::cout << tempBits.to_string() << std::endl;

	return true;
}

// Remove k from the set if it is a member
bool NB_Hashtable::Erase(WORD_SIZE_TYPE k)
{
	WORD_SIZE_TYPE h = std::hash<WORD_SIZE_TYPE>{}(k);
	// scan probe sequence
	WORD_SIZE_TYPE max = GetProbeBound(h);
	WORD_SIZE_TYPE temp = (k | MEMBER);

	for (WORD_SIZE_TYPE i = 0; i <= max; i++)
	{
		// remove a copy of <k, member>
		// May have to modify this to specify that k's status must be member 
		// if that is not a pre-condition for Erase().
		if (*Bucket(h, i) == /*k*/ temp)
		{
			// Set status bit to busy (01)
			if (std::atomic_compare_exchange_strong(Bucket(h, i), &temp, BUSY))
			{
				ConditionallyLowerBound(h, i);
				*Bucket(h, i) = EMPTY;
				return true;
			}
		}
	}

	return false;
}




// Private member functions

// Sets the bound entry bound and scanning bit to <false, 0>
void NB_Hashtable::InitProbeBound(WORD_SIZE_TYPE h)
{
	bounds[h % size] = SCAN_FALSE;
}

// Returns the maximum offset of any collision in a probe sequence as well as
// that bound's scanning bit <bound, scanning>
WORD_SIZE_TYPE NB_Hashtable::GetProbeBound(WORD_SIZE_TYPE h)
{
	return bounds[h % size];
}

// NOTE FOR DEVELOPER (JON): Index has to be WORD_SIZE_TYPE (uint64_t) instead
// of an int because it refers at a memory location AT index in an array.
// Ensure maximum >= index
void NB_Hashtable::ConditionallyRaiseBound(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index)
{
	WORD_SIZE_TYPE old_bound, new_bound;

	do {
		old_bound = bounds[h % size];
		new_bound = std::max(old_bound, index);
	} while (!std::atomic_compare_exchange_weak(&(bounds[h % size]), &old_bound, new_bound));
}

// Allow maximum < index
void NB_Hashtable::ConditionallyLowerBound(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index)
{
	WORD_SIZE_TYPE bound = bounds[h % size], expectedFalse, expectedTrue;

	// If scanning bit is set, unset it
	if ((bound & SCAN_TRUE) == (SCAN_TRUE))
	{
		std::atomic_compare_exchange_weak(&bounds[h % size], &bound, (bound & ~SCAN_TRUE));
	}
	
	expectedFalse = index & ~SCAN_TRUE;
	if (index > 0)
	{
		while (std::atomic_compare_exchange_weak(&bounds[h % size], &expectedFalse, (index | SCAN_TRUE)))
		{
			WORD_SIZE_TYPE i = index - 1;
			while ((i > 0) && (!DoesBucketContainCollisions(h, i)))
			{
				i--;
			}
			expectedTrue = index | SCAN_TRUE;
			std::atomic_compare_exchange_strong(&bounds[h % size], &expectedTrue, (i & ~SCAN_TRUE));
		}
	}
}

// Return bucket entry at hash value plus offset (using quadratic probing)
std::atomic<BucketT>* NB_Hashtable::Bucket(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index)
{
	return &buckets[(h + index * (index + 1) / 2) % size];
}

// In the paper, - refers to the empty state in <key, state>. There are 4
// states: empty, busy, inserting, and member. The paper was written in 2005
// and more than on a 32-bit system. This means that there are 2 unused bits
// at the end of every address---just enough to identify these 4 states. 
// Tentatively, here the codes I am using for my state codes:
// empty = 000 => EMPTY = 0x0000000000000000
// busy = 001 => BUSY = 0x2000000000000000
// collided = 010 => COLLIDED = 0x4000000000000000
// visible = 011 => VISIBLE = 0x6000000000000000
// inserting = 100 => INSERTING = 0x8000000000000000
// member = 101 => MEMBER = 0xA000000000000000
bool NB_Hashtable::DoesBucketContainCollisions(WORD_SIZE_TYPE h, WORD_SIZE_TYPE index)
{
	// <state, version> bit structure:
	// First two bits are state, rest are for version. So, in:
	// 1110 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000
	// the 1s are the state portion and the 0s are for the version.
	BucketT temp = *Bucket(h, index);
	// <state1, version1>
	WORD_SIZE_TYPE vs1 = temp.vs;
	/* // Recover key from <state, key>
	WORD_SIZE_TYPE key = k & ~MEMBER;
	return ((k != EMPTY) && (std::hash<WORD_SIZE_TYPE>{}(key) == h)); */
}