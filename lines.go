package main

import "math/rand/v2"

// GrootLines are prepared "I am Groot" variants. Punctuation, CAPS, and spacing
// stand in for emotion — same three words, infinite vibes.
var GrootLines = []string{
	"I am Groot.",
	"I am Groot!",
	"I am Groot?",
	"I am Groot...",
	"I AM GROOT!",
	"I AM GROOT!!!",
	"i am groot",
	"I  am  Groot.",
	"I am  Groot",
	"I. am. Groot.",
	"I am GROOT.",
	"I am Groot‽",
	"…I am Groot…",
	"I am Groot!!",
	"I am Groot.",
	"I am groot.",
	"I AM Groot!",
	"I am Groot~",
	"I   am   Groot.",
	"I am Groot…",
	"I am GROOT!!!",
	"I am Groot :)",
	"I.am.Groot.",
	"I am Groot,",
	"I am Groot!!?",
}

// PickLine returns a random prepared Groot line.
func PickLine() string {
	return GrootLines[rand.IntN(len(GrootLines))]
}
