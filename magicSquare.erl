%%%--------------------------------------------------------------------------
%%%	Algorithmus fuer die verteilte Berechnung Magischer Quadrate
%%%--------------------------------------------------------------------------
-module(magicSquare).
-compile(export_all).

test() -> eunit:test(ms_tests). 

%%----------------------------------------------------------------------
%% Schreibt eine Liste auf die Standardausgabe.
%%
%% Args:    List    Die Liste
%%
%% Returns: Alle moeglichen magischen Zeilen.
%%----------------------------------------------------------------------
printList([])    -> io:format("~s~n", ["ENDE"]);
printList([H|T]) -> io:format("~p~n", [H]), printList(T).

%%%--------------------------------------------------------------------------
%%% Berechnung magischer Quadrate auf einem Rechner ohne Verteilung
%%%--------------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Berechnet alle moeglichen Zeilen eines magischen Quadrats.
%%
%% Args:    Size        Die groesse des Quadrats
%%          MagicNumber Die magische Zahl des Quadrats.
%%
%% Returns: Alle moeglichen magischen Zeilen.
-spec row(non_neg_integer(), non_neg_integer()) ->
    list(list(non_neg_integer())).
%%----------------------------------------------------------------------
row(Size, MagicNumber) ->
    Combs = row(Size, MagicNumber, lists:seq(1, Size*Size)),
    PosRows = lists:filter(
                fun(Element)-> lists:sum(Element) == MagicNumber end, Combs),
    lists:filter(
      fun(Element)-> length(lists:usort(Element)) == Size end, PosRows).

row(0, _, _) -> [[]];
row(Size, MagicNumber, Elements) ->
    [ B++[A] || B <- row(Size-1, MagicNumber, Elements), A <- Elements].

%%----------------------------------------------------------------------
%% Ueberprueft ob sich in zwei Listen doppelte Elemente befinden.
%%
%% Args:    Liste1  Die erste Liste.
%%          Liste2  Die zweite Liste.
%%
%% Returns: true wenn beide Listen mindestens ein gleiches Element 
%%          besitzen. Ansonsten false.
-spec duplicate(list(non_neg_integer()),list(non_neg_integer())) ->
    true | false.
%%----------------------------------------------------------------------
duplicate(Liste1, Liste2) ->
    lists:any(fun (E) -> lists:member(E, Liste2) end, Liste1).

%%----------------------------------------------------------------------
%% Setzt eine beliebige Anzahl von magischen Zeilen zusammen.
%%
%% Args:    Col         Anzahl der magischen Zeilen,
%%                      die kombiniert werden.
%%          Size        Die groesse des Quadrats.
%%          MagicNumber Die magische Zahl.
%%
%% Returns: Liste mit den zusammengesetzen magischen Zeilen.
-spec combineRows(
        non_neg_integer(), non_neg_integer(), non_neg_integer(),
        list(non_neg_integer())) -> list(list(non_neg_integer())).
%%----------------------------------------------------------------------
combineRows(Col,Size,MagicNumber, Elems) ->
    combineRows(Col, row(Size, MagicNumber)).

combineRows(Col, Size, MagicNumber) ->
    combineRows(Col, Size, MagicNumber, lists:seq(1,Size*Size)).

combineRows(0, _) -> [[]];
combineRows(Col, Rows) ->
    [ A++B ||
      B <- combineRows(Col-1, Rows), A <- Rows, duplicate(A,B) == false ].

%%----------------------------------------------------------------------
%% Berechnet aus einem Teilquadrat alle moeglichen gueltigen
%% magischen Quadrate.
%%
%% Args:    Part        Das Teilquadrat.
%%          Size        Die groesse des Quadrats.
%%          MagicNumber Die magische Zahl.
%%
%% Returns: Liefert alle moeglichen Quadrate die sich bilden lassen,
%%          oder eine leere Liste.
-spec calcSquares(
        list(non_neg_integer()), non_neg_integer(), non_neg_integer()) ->
            list(list(non_neg_integer())).
%%----------------------------------------------------------------------
calcSquares(Part, Size, MagicNumber)->
  Rows = combineRows(Size - trunc(length(Part)/Size)-1,
                     Size, MagicNumber,
                     lists:seq(1,Size*Size)),
  PossibleSquares = [ Part++Row ||
                      Row <- Rows , duplicate(Part,Row) == false ],
  Squares = [ S++LastRow ||
              S <- PossibleSquares,
              LastRow <- [calcLastRow(S,Size,MagicNumber)],
              lists:sum(LastRow) == MagicNumber
              andalso length(lists:usort(LastRow)) == Size
              andalso duplicate(LastRow,S) == false],
  checkMagicSquares(Squares, Size, MagicNumber).
%%----------------------------------------------------------------------
%% Berechnet alle moeglichen letzten magische Zeilen
%% eins magischen Quadrats.
%%
%% Args:    Part        Das Teilquadrat.
%%          Size        Die groesse des Quadrats.
%%          MagicNumber Die magische Zahl.
%%
%% Returns: Alle moeglichen letzten magischen Zeilen.
%%----------------------------------------------------------------------
calcLastRow(Part, Size, MagicNumber) ->
    [ MagicNumber - Sum  ||
      Column <- lists:seq(1,Size),
      Sum <- [lists:sum(getColumn(Part, Size, Column))],
      lists:member(MagicNumber-Sum, lists:seq(1,Size*Size)) ].


checkMagicSquares([], _, _) -> [];
checkMagicSquares(Squares, Size, MagicNumber) ->
  [ S || S <- Squares,
    sumDiagonalLR(S, Size, 1) == MagicNumber,
    sumDiagonalRL(S, Size, Size) == MagicNumber].

checkColumns(_, [], _) -> false;
checkColumns(SquareSize, Square, MagicNumber) ->
    Columns = [ getColumn(Square,SquareSize,X) ||
                    X <- lists:seq(1, SquareSize) ],
    Test = lists:takewhile(
             fun(X) -> lists:sum(X) == MagicNumber end, Columns),
    case (length(Test) == SquareSize) of
        true -> true;
        false -> false
    end.
%%----------------------------------------------------------------------
%% Liefert eine Spalte eines magischen Quadrats.
%%
%% Args:    Square      Das magische Quadrat.
%%          RowLength   Die Anzahl Zeilen des magischen Quadrats.
%%          Column      Die Splate die geliefert werden soll.
%%
%% Returns: Die Spalte des Quadrats.
%%----------------------------------------------------------------------
getColumn(Square, RowLength, Column) ->
    [ lists:nth(X+(Column-1), Square) ||
        X <- lists:seq(1, length(Square), RowLength)].
%%----------------------------------------------------------------------
%% Berechnet die Summe der Hauptdiagonalen \ eines magischen Quadats.
%%
%% Args:    Square  Das magische Quadrat.
%%          Size    Die groesse des Quadrats.
%%          Step    Die Schrittweite.
%%
%% Returns: Die Summe der Diagonalen.
%%----------------------------------------------------------------------
sumDiagonalLR([], _,_) -> 0;
sumDiagonalLR(_,Size,Step) when Size < Step -> 0;
sumDiagonalLR(Square, Size, Step)->
    lists:nth(
      Size*(Step-1) + Step, Square) + sumDiagonalLR(Square, Size, Step +1).
%%----------------------------------------------------------------------
%% Berechnet die Summe der Hauptdiagonalen / eines magischen Quadats.
%%
%% Args:    Square  Das magische Quadrat.
%%          Size    Die groesse des Quadrats.
%%          Step    Die Schrittweite.
%%
%% Returns: Die Summe der Diagonalen.
%%----------------------------------------------------------------------
sumDiagonalRL([], _,_) -> 0;
sumDiagonalRL(_,_,Step) when Step < 1 -> 0;
sumDiagonalRL(Square, Size, Step)->
    lists:nth(
      Size*(Step-1) + (Size-Step+1), Square) +
        sumDiagonalRL(Square, Size, Step - 1).
%%----------------------------------------------------------------------
%% Ermittelt aus einer Liste von Teilquadraten, alle magischen
%% Quadrate die sich bilden lassen.
%%
%% Args:    Liste       Die Liste der Teilquadrate.
%%          Size        Die groesse der Quadrate.
%%          MagicNumber Die magische Zahl.
%%          Num         Anzahl, der gefundenen magischen Quadrate.
%%
%% Returns: Liste aller gefundenen magischen Quadrate.
-spec combineSquares(
        list(list(non_neg_integer())), non_neg_integer(), non_neg_integer(),
        integer()) -> list(list((non_neg_integer()))).
%%----------------------------------------------------------------------

%% debug mode
combineSquares([],_, _, _) -> [];
combineSquares([X|XS], Size, MagicNumber, Num) ->
  %statistics(runtime),
  Res = calcSquares(X,Size,MagicNumber),
  %{_, Time1} = statistics(runtime),
  %U= Time1/ 1000,
  %io:format("calcSquare time:~p",[U]),
  case Res of
    %[] -> io:format(" | kein quadrat gefunden mit: ~p~n", [X]),
    %       combineSquares(XS, Size, Value, Num);
    [] -> combineSquares(XS, Size, MagicNumber, Num);
    _ -> io:format("Erg Nummer~p:~p~n",[Num,Res]),
         Res++combineSquares(XS, Size, MagicNumber,Num+length(Res))
  end.

%% normal mode
combineSquares(Parts, Size, Value) ->
  lists:flatmap(fun(X)->calcSquares(X,Size,Value) end, Parts).
%%----------------------------------------------------------------------
%% Berechnet magische Quadrate einer bestimmten Ordnung.
%%
%% Args:    Size Die groesse der Quadrate.
%%          Mode debug = mit Augaben.
%%
%% Returns: Alle moeglichen magischen Quadrate.
%%----------------------------------------------------------------------
magicsquare(Size)-> magicsquare(Size, egal).
magicsquare(Size, Mode)->
    statistics(runtime),
    Result = case Mode of
                 debug ->  case Size of
                               3 -> Parts = combineRows(1,3,15),
                                   combineSquares(Parts,3,15,0);
                               4 -> Parts= combineRows(2,4,34),
                                   combineSquares(Parts,4,34,0);
                               _ -> error
                           end;
                 _ -> case Size of
                          3 -> Parts = combineRows(2,3,15),
                              combineSquares(Parts,3,15);
                          4 -> Parts = combineRows(2,4,34),
                              combineSquares(Parts,4,34);
                          _ -> error
                      end
                 end,
    {_, Time1} = statistics(runtime),
    U= Time1/ 1000,
    io:format("Anzahl der Quadrate:~p~n",[length(Result)]),
    io:format("Magicsquare Time:~p~n",[U]),
    Result.

%%%==========================================================================
%%%--------------------------------------------------------------------------
%%% Berechnung magischer Quadrate auf einem Rechner + Verteilung
%%%--------------------------------------------------------------------------
%%----------------------------------------------------------------------
%% Berechnung magischer Quadrate mit mehreren Prozessen auf einem
%% Rechner.
%%
%% Args:    Size        Die groesse der Quadrate.
%%          Prozesse    Anzahl der verwendete Prozesse.
%%
%% Returns: Alle moeglichen magischen Quadrate.
-spec distribMS(non_neg_integer(), non_neg_integer()) ->
    list(list(non_neg_integer())).
%%----------------------------------------------------------------------
distribMS(Size, Prozesse) ->
    statistics(runtime),
    Result =
        case Size of
            3 -> Value=15, PSquare=combineRows(1,Size,Value),
                 spawn_at(Prozesse, node(), PSquare, 3, Value, init_local),
                 loop_gather(Prozesse,[]);
            4 -> Value=34, PSquare=combineRows(2,Size,Value),
                 spawn_at(Prozesse, node(), PSquare, 4, Value, init_local),
                 loop_gather(Prozesse,[]);
            _ ->  [[]]
        end,
    {_, Time1} = statistics(runtime),
    U= Time1/ 1000,
    io:format("Anzahl der Quadrate:~p~n",[length(Result)]),
    io:format("Magicsquare Time:~p~n",[U]),
    Result.
%%----------------------------------------------------------------------
%% Erzeugt eine bestimmte Anzahl von Prozessen auf einem Host.
%%
%% Args:    Prozesse    Anzahl der zu erzeugenden Prozesse.
%%          Host        Host, auf dem die Prozesse erzeugt werden.
%%          SquareList  Liste von Teilquadraten.
%%          size        Die groesse der Quadrate.
%%          MagicNumber Die magische Zahk.
%%          InitFun     Funktion, die jeder Prozess ausfuehrt.
%%
%% Returns: Glaube eine Liste von PIDs :D
-spec spawn_at(integer(), atom(), list(list(non_neg_integer())),
               non_neg_integer(), non_neg_integer(), atom()) -> ok.
%%----------------------------------------------------------------------
spawn_at(0, _, _, _, _, _) -> [];
spawn_at(_, _, [], _, _, _) -> [];
spawn_at(Prozesse, Host, SquareList, Size, MagicNumber, InitFun)->
  Parts = createPartitions(Prozesse, SquareList),
  case InitFun of
    init_local ->
      [spawn(Host, bel3, InitFun,
             [X, self(), lists:nth(X,Parts), Size, MagicNumber, Host]) ||
              X <- lists:seq(1, Prozesse)];
    init_global ->
      [spawn(Host, bel3, InitFun,
             [X, self(), lists:nth(X,Parts), Size, MagicNumber, Host]) ||
              X <- lists:seq(1, Prozesse)]
  end.
%%----------------------------------------------------------------------
%% Spaltet eine Liste in Teillisten.
%%
%% Args:    Parts   Anzahl, in die die uebergebene Liste zerlegt
%%                  werden soll.
%%          List    Liste die aufgespalten wird.
%%
%% Returns: Liste mit den jeweiligen Teillisten.
%%----------------------------------------------------------------------
createPartitions([], _) -> [];
createPartitions(_, []) -> [];
createPartitions(Parts, List) -> ElementsInPart = round(length(List) / Parts),
  PartedList = [ lists:sublist(List, X, ElementsInPart) ||
                 X <- lists:seq(1,length(List),ElementsInPart) ],
  case (length(PartedList) > Parts) of
    true -> More = lists:sublist(PartedList, Parts+1, length(PartedList)),
            NewParted = PartedList--More; % Dadurch Fehlen einige :/
    false -> PartedList
  end.

% Methode, die bei Abspaltung des Prozesses aufgerufen wird
% hat die/den Parameter [Nr, SPid, PList, Size, Value, Host]
% Die Methode berechnet fuer eine Menge an Teilquadraten alle Loesungen und
% sendet diese an den erzeugenden Prozess.
%%----------------------------------------------------------------------
%% Wird bei lokaler Prozesserzeugung aufgeruden.
%%
%% Args:    Nr          Nummer des Prozesses.
%%                      (nur fuer debug-Ausgaben auf der Konsole)
%           SPid        Prozessnummer des erzeugenden Prozesses.
%           SquareList  Teilliste, fuer die ein Prozess die
%                       magischen Quadrate berechnen soll
%           Size        Die groesse des Quadrats.
%           MagicNumber Die magische Zahl.
%           Host        ???
%
%% Returns: /
%%----------------------------------------------------------------------
init_local(Nr, SPid, SquareList, Size, MagicNumber,_)->
  distrib_calc_squares(Nr, SPid, SquareList, Size, MagicNumber).
%%----------------------------------------------------------------------
%% Berechnet die moeglischen magischen Quadrate und
%% sendet diese an den Oberprozess zurueck.
%%
%% Args:    Nr          Nummer des Prozesses.
%%                      (nur fuer debug-Ausgaben auf der Konsole)
%           SPid        Prozessnummer des erzeugenden Prozesses.
%           SquareList  Teilliste, fuer die ein Prozess die
%                       magischen Quadrate berechnen soll
%           Size        Die groesse des Quadrats.
%           MagicNumber Die magische Zahl.
%
%% Returns: /
-spec distrib_calc_squares(non_neg_integer(), pid(),
                           list(list(non_neg_integer())),
                           non_neg_integer(), non_neg_integer()) -> ok.
%%----------------------------------------------------------------------
distrib_calc_squares(Nr, SPid, SquareList, Size, MagicNumber)->
  SPid!{Nr, combineSquares(SquareList, Size, MagicNumber,0)}.
%%----------------------------------------------------------------------
%% Sammelt alle Ergebnisse der erzeugten Prozesse ein.
%%
%% Args:    Prozesse Die Anzahl erzeugter Prozesse.
%%          Result   Sammler, zum mitfuehren der Ergebnismenge.
%%
%% Returns: Alle moeglichen magischen Quadrate.
-spec loop_gather(non_neg_integer(), list(list(non_neg_integer()))) ->
    list(list(non_neg_integer())).
%%----------------------------------------------------------------------
loop_gather(0, Result) -> Result;
loop_gather(Prozesse,Result)->
  receive
    {"nodeMainProcess", List} -> loop_gather(Prozesse-1, Result++List);
    {Nr,List} -> R = mergeSquareList(List, Result), loop_gather(Prozesse-1, R)
  end.

mergeSquareList([], Acc) -> Acc;
mergeSquareList([H|T], Acc) -> case H == [] of
                                 true -> mergeSquareList(T,Acc);
                                 false -> mergeSquareList(T,Acc++H)
                               end.

%%%==========================================================================
%%%--------------------------------------------------------------------------
%%% Berechnung magischer Quadrate auf mehreren Rechnern
%%%--------------------------------------------------------------------------
%%----------------------------------------------------------------------
%% Hosts auf denen die Verteilung ausgefuehrt wird.
%%
%% Args:    /
%%
%% Returns: Liste der Hosts.
%%----------------------------------------------------------------------
%hosts() -> [{'alice@dumbo01.f4.htw-berlin.de',48},
%             {'bob@dumbo01.f4.htw-berlin.de',48}].
hosts()->[{'alice@mastersword',10},
          {'bob@mastersword',10}].
%%----------------------------------------------------------------------
%% Berechnet die Anzahl aller Prozesse die erzeugt werden sollen.
%%
%% Args:    /
%%
%% Returns: Summe aller Prozesse.
%%----------------------------------------------------------------------
c_count()-> lists:sum([Count||{_,Count}<-hosts()]).
%%----------------------------------------------------------------------
%% Berechnung magischer Quadrate verteilt auf mehreren Rechnern.
%%
%% Args:    Size    Die groesse der Quadrate.
%%
%% Returns: Alle moeglichen magischen Quadrate.
%%----------------------------------------------------------------------
megaDistribMS(Size)->
  % Ausschalten des Error-Loggings auf der Konsole
  error_logger:tty(false),
  register(host_monitor,spawn(fun()->init_host_monitor(hosts()) end)),
  statistics(runtime),
  io:format("Hauptprozess: ~p~n", [self()]),
  Result=
    case Size of
      3 -> Value=15, PSquare=combineRows(2,Size,Value),
        while(c_count(), hosts(), PSquare, 3, 15),
        loop_gather(length(hosts()),[]);
      4 -> Value=34, PSquare=combineRows(2,Size,Value),
        while(c_count(), hosts(), PSquare, 4, 34),
        loop_gather(length(hosts()),[]);
      _ ->  [[]]
    end,
  {_, Time1} = statistics(runtime),
  U= Time1/ 1000,
  io:format("Anzahl der Quadrate:~p~n",[length(Result)]),
  io:format("Magicsquare Time:~p~n",[U]),
  host_monitor!stop,
  Result.

% Schleife fuer das spawnen der Prozesse auf mehreren Rechnern
% Benutzt die Methode spawn_at(...)
% Aufruf: while (CCount, Hosts, PList, Size, Value)
% CCount - Anzahl der Prozesse die gespawnt werden sollen
% Hosts - Hostliste der Form { VM-Name, Anzahl der Prozesse}
% PList - Liste der Teilquadrate
% Size - Anzahl der Elemente, die berechnet werden sollen
% Value - Wert der Summe der Zeile
-spec while(non_neg_integer(), list({atom(),non_neg_integer()}), list(list(non_neg_integer())), non_neg_integer(),non_neg_integer())->ok.

while(CCount, Hosts, PList, Size, Value) ->
  PartedList = createPartitions(length(hosts()), PList),
  spawn_at_nodes(Hosts, PartedList, Size, Value).

spawn_at_nodes([], _, _, _) -> io:format("Hauptprozesse auf Nodes gespawnt~n"),
  io:format("Connected Nodes: ~p~n", [nodes()]);
spawn_at_nodes([{Host,_}|RestHosts], [Part|RestPartedList], Size, Value) ->
  spawn_at(1, Host, Part, Size, Value, init_global),
  spawn_at_nodes(RestHosts, RestPartedList, Size, Value).


% Supervisor-Prozess, der die Ausfuehrung der Berechnungen ueberwacht
% Spawnt die Berechnungsprozesse auf den Nodes des Erlang-Clusters und behandelt die Fehlerfaelle
% Nr - Nummer des Prozesses (nur zur besseren Identifikation)
% SPid - Prozessnummer des erzeugenden Prozesses
% PList - Teilliste, fuer die ein Prozess die magischen Quadrate berechnen soll
% Size - Anzahl der Spalten/Zeilen
% Value - Wert der Summe der Zeile
% Try - Anzahl der noch ausstehenden Versuche

init_global(Nr, SPid, PList, Size, Value, Host)->
  init_global(Nr, SPid, PList, Size, Value, Host, 3).

-spec init_global(non_neg_integer(), pid(), list(list(non_neg_integer())), non_neg_integer(), non_neg_integer(),
    atom(), non_neg_integer()) -> ok.

% "Hauptprozess" auf der jeweiligen Node
% erhält die ergebnisse der Sub-Prozesse
init_global(Nr, SPid, PList, Size, Value, Host, Try)->
  io:format("~p spawnt ~p Prozesse auf ~p ~n", [node(), 5, Host]),
  {_,Processes} = lists:keyfind(node(), 1, hosts()),
  spawn_at(Processes, node(), PList, Size, Value, init_local),
  Res = loop_gather(Processes,[]),
  SPid!{"nodeMainProcess", Res}.




% Monitoring-Prozess fuer die Ueberwachung der zur Verfuegung stehenden Cluster-Nodes
% Er wird von der Hauptmethode megaDistribMS gestartet,
% Der Prozess kann ueber das Atom host_monitor angesprochen werden.
% Er beinhaltet die folgenden Operationen:
%  getnode - Ermittlung eines verfuegbaren Nodes
%  addnode - Hinzunahme eines Nodes
%  gethosts - Ermittlung aller verfuegbaren Hosts
%  deletenode - Loeschen eines Nodes

init_host_monitor(MonitorList) -> ML= lists:map(fun({Host,_})->Host end, MonitorList),
  lists:foreach(fun(Host)->erlang:monitor_node(Host, true) end, ML),
  monitorHosts(ML).

monitorHosts([])-> erlang:error(no_hosts_available);
monitorHosts(HostList)->
  receive
    {nodedown, NodeName}-> io:format("Host ~p is down!~n",[NodeName]),
      monitorHosts(lists:delete(NodeName, HostList));
    {getnode, From}-> io:format("Host ~p is requested!~n",[hd(HostList)]),
      From!{newhost, hd(HostList)}, monitorHosts(tl(HostList)++[hd(HostList)]);
    {addnode, NodeName}-> io:format("Host ~p is added!~n",[NodeName]),
      monitor_node(NodeName, true),
      monitorHosts([NodeName|HostList]);
    {gethosts, From} -> io:format("Hosts angefordert!~n"),
      From!{hostlist, HostList}, monitorHosts(HostList);
    {deletenode, NodeName}-> io:format("Host ~p will be deleted!~n",[NodeName]),
      monitorHosts(lists:delete(NodeName, HostList));
    stop -> ok
  end.
