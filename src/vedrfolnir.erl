-module(vedrfolnir).
-compile(export_all).

%% ============================================================================
%% Application
%% ============================================================================

start() ->
  Coordinator = spawn(?MODULE, coordinator, []),
  { ok, Coordinator }.

stop(Coordinator) ->
  exit(Coordinator, kill).

%% ============================================================================
%% Coordinator
%% ============================================================================

coordinator() ->
  Cache = spawn_link(?MODULE, cache, []),
  Upstream = spawn_link(?MODULE, upstream, []),
  Server = spawn_link(?MODULE, server, [ self() ]),
  coordinator(Server, Upstream, Cache).

coordinator(Server, Upstream, Cache) ->
  case read_from_socket() of
    { ok, Key, SocketSpec } ->
      case read_from_cache(Cache, Key) of
        { ok, undefined } ->
          case read_from_upstream(Upstream, Key) of
            { ok, undefined } ->
              write_to_socket(SocketSpec, undefined);
            { ok, UpstreamValue } ->
              write_to_cache(Cache, Key, UpstreamValue),
              write_to_socket(SocketSpec, UpstreamValue);
            Other ->
              io:format("Unknown message from upstream: ~p.~n", [ Other ])
          end;
        { ok, CacheValue } ->
          write_to_socket(SocketSpec, CacheValue);
        Other ->
          io:format("Unknown message from cache: ~p.~n", [ Other ])
      end;
    Other ->
      io:format("Unknown message from socket: ~p.~n", [ Other ])
  end,
  coordinator(Server, Upstream, Cache).

read_from_socket() ->
  receive Response -> Response end.

write_to_socket(SocketSpec, Value) ->
  { Socket, Host, Port } = SocketSpec,
  gen_udp:send(Socket, Host, Port, Value).

read_from_cache(Cache, Key) ->
  Cache ! { self(), { get, Key } },
  receive Response -> Response end.

write_to_cache(Cache, Key, Value) ->
  Cache ! { set, Key, Value }.

read_from_upstream(Upstream, Key) ->
  Upstream ! { self(), { get, Key } },
  receive Response -> Response end.

%% ============================================================================
%% Server
%% ============================================================================

server(Coordinator) ->
  { ok, Socket } = gen_udp:open(6000, [ binary ]),
  server(Coordinator, Socket).

server(Coordinator, Socket) ->
  receive
    { udp, _, Host, Port, Binary } ->
      Coordinator ! { ok, Binary, { Socket, Host, Port } };
    Other ->
      Other
  end,
  server(Coordinator, Socket).

%% ============================================================================
%% Upstream
%% ============================================================================

upstream() ->
  { ok, Socket } = gen_udp:open(0, [ binary, inet ]),
  receive
    { Sender, { get, Key } } ->
      ok = gen_udp:send(Socket, "127.0.0.1", 5400, Key),
      receive
        { udp, _, _, _, Value } ->
          Sender ! { ok, Value };
        _Other ->
          Sender ! { ok, undefined } % TODO
      %after 1000 ->
      %  Sender ! { ok, undefined } % TODO
      end
  end,
  gen_udp:close(Socket),
  upstream().

%% ============================================================================
%% Cache
%% ============================================================================

cache() ->
  { ok, Redis } = eredis:start_link("127.0.0.1", 6379, 1),
  cache(Redis).

cache(Redis) ->
  receive
    { Sender, { get, Key } } ->
      case eredis:q(Redis, [ "GET", Key ]) of
        { ok, Value } ->
          Sender ! { ok, Value };
        { error, _Reason } ->
          Sender ! { ok, undefined } % TODO
      end;
    { set, Key, Value } ->
      eredis:q(Redis, [ "SET", Key, Value ])
  end,
  cache(Redis).
