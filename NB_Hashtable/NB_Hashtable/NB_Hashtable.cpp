// Jonathan White, Derek Spooner
// COP 4250 - Spring 2018
// main.cpp - Defines the entry point for the console application.

#include "stdafx.h"
#include "NB_Hashtable.h"
#include <bitset>

void testCodeBackup();

int main()
{
	NB_Hashtable* h = new NB_Hashtable();
	h->Init();

	if (h->Insert(121))
	{
		std::cout << "121 successfully inserted!" << std::endl;
	}

	if (!h->Insert(121))
	{
		std::cout << "As fun as it would be, I can't insert 121 twice." << std::endl;
	}

	if (h->Lookup(121))
	{
		std::cout << "121 successfully found!" << std::endl;
	}

	if (h->Erase(121))
	{
		std::cout << "121 successfully erased!" << std::endl;
	}

	if (!h->Lookup(121))
	{
		std::cout << "I couldn't find 121 (like we expected ;-))" << std::endl;
	}

    return 0;
}

void testCodeBackup()
{
	uint64_t test = 0x8000000000000000;

	if ((test & SCAN_TRUE) == SCAN_TRUE)
	{
		std::cout << "At least this stuff is working!" << std::endl;
	}

	unsigned long long empty = 0x0000000000000000;
	unsigned long long busy = 0x4000000000000000;
	unsigned long long inserting = 0x8000000000000000;
	unsigned long long member = 0xC000000000000000;

	std::bitset<64> emptyBits(empty);
	std::bitset<64> busyBits(busy);
	std::bitset<64> insertingBits(inserting);
	std::bitset<64> memberBits(member);

	std::cout << "Empty: " << emptyBits.to_string() << std::endl;
	std::cout << "Busy: " << busyBits.to_string() << std::endl;
	std::cout << "Inserting: " << insertingBits.to_string() << std::endl;
	std::cout << "Member: " << memberBits.to_string() << std::endl;

	std::cout << std::endl;
	std::bitset<64> negatedTrueBits(~SCAN_TRUE);
	std::cout << negatedTrueBits.to_string() << std::endl;
}