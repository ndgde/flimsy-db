package indexer

type IndexableInt32 int32

func (a IndexableInt32) Equal(other IndexableInt32) bool          { return a == other }
func (a IndexableInt32) Less(other IndexableInt32) bool           { return a < other }
func (a IndexableInt32) LessOrEqual(other IndexableInt32) bool    { return a <= other }
func (a IndexableInt32) Greater(other IndexableInt32) bool        { return a > other }
func (a IndexableInt32) GreaterOrEqual(other IndexableInt32) bool { return a >= other }

type IndexableFloat64 float64

func (a IndexableFloat64) Equal(other IndexableFloat64) bool          { return a == other }
func (a IndexableFloat64) Less(other IndexableFloat64) bool           { return a < other }
func (a IndexableFloat64) LessOrEqual(other IndexableFloat64) bool    { return a <= other }
func (a IndexableFloat64) Greater(other IndexableFloat64) bool        { return a > other }
func (a IndexableFloat64) GreaterOrEqual(other IndexableFloat64) bool { return a >= other }

type IndexableString string

func (a IndexableString) Equal(other IndexableString) bool          { return a == other }
func (a IndexableString) Less(other IndexableString) bool           { return a < other }
func (a IndexableString) LessOrEqual(other IndexableString) bool    { return a <= other }
func (a IndexableString) Greater(other IndexableString) bool        { return a > other }
func (a IndexableString) GreaterOrEqual(other IndexableString) bool { return a >= other }
