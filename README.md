About
=====

`Celina` is a cell phone invoice analyser I wrote some years ago. It parses PDF
files of a defined format (the format my former provider used) and stores the
extracted data in an sqlite database. This then allows for calculation of
statistics on the gathered data.

My intention was to get a grip on what my usual cell phone data actually looks
like to be able to make a more informed, rational decision on what I would want
from my next contract.

Issues
======

 * So far `celina` is neither configurable through a file, nor does it sport a
   plugin architecture for parser modules. So enabling it to parse a different
   invoice format will essentially mean adjusting regexs in the code. But while
   we are at it, we might as well make the whole mechanism more generic.
 * `Celina` uses [elixir](http://elixir.ematia.de/trac/wiki) as an
   [ORM](http://en.wikipedia.org/wiki/Object-relational_mapping), so it should
   be easy to swap [sqlite](http://www.sqlite.org/) for another
   [DBMS](http://en.wikipedia.org/wiki/Database_management_system) (yes, we
   would want that to be configurable as well), but so far there is no way to
   remove data from the data base (not even to overwrite it).
