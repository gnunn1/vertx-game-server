import io.vertx.core.CompositeFuture as CompositeFuture
import io.vertx.core.eventbus.Message as Message
import io.vertx.lang.groovy.GroovyVerticle as GroovyVerticle
import io.vertx.core.Handler as Handler
import io.vertx.core.json.Json as Json
import io.vertx.core.eventbus.EventBus as EventBus
import java.util.logging.Logger as Logger
import com.redhat.middleware.keynote.GameUtils as GameUtils
import io.vertx.core.shareddata.AsyncMap as AsyncMap
import io.vertx.core.http.HttpClient as HttpClient
import io.vertx.core.Future as Future
import java.util.logging.Level as Level
import java.util.Map as Map
import io.vertx.core.AsyncResult as AsyncResult

public class GameVerticle extends GroovyVerticle { 

    private final static Logger LOGGER = Logger.getLogger(GameVerticle.class.getName());

    // Configuration and constants
    public static final Map<String, Serializable> DEFAULT_CONF = [
            'opacity'          : 85,
            'scale'            : 0.3F,
            'speed'            : 50,
            'background'       : "default",
            'points'           : [
                    'red'         : 1,
                    'blue'        : 1,
                    'green'       : 1,
                    'yellow'      : 1,
                    'goldenSnitch': 50
            ],
            'goldenSnitch'     : false,
            'trafficPercentage': 100
    ]

    private static final Map<String, Map<String, Player>> TEAM_PLAYERS = new HashMap<>();

    static final String TEAM_COUNTER_NAME = "redhat.team";
    static final String TEAM_POP_COUNTER_NAME = "redhat.team.pop";
    static final String PLAYER_NAME_MAP = "redhat.player.name";
    static final String GAME_STATE_MAP = "redhat.game.state";

    private int num_teams 
    private int score_broadcast_interval 
    private String configurationHost 
    private int configurationPort 
    private String configurationPath 
    private String achievementHost 
    private int achievementPort 
    private String achievementPath 
    private String scoreHost 
    private int scorePort 
    private String scorePath 
    private String scoreAuthHeader

    // Verticle fields

    // Shared
    Map<Integer, Team> teams = [:];
    // Shared
    List<Admin> admins = [];

    def counter;
    def teamCounters = [:];
    def teamPopCounters = [:];

    Map adminConfiguration = [:]
        
    private AsyncMap<String, String> playerNames 
    private AsyncMap<String, String> clusteredState 
    private HttpClient mechanicsClient 
    private HttpClient achievementClient 
    private HttpClient scoreClient 
    private java.lang.Object state 
    private java.lang.Object selfieState 
    private java.lang.Object authToken 

    @Override
    public void start(Future<Void> future) throws Exception {
        LOGGER.setLevel( Level .INFO)

        if ( System .env.get('AUTH_TOKEN') != null) {
            authToken  =  System .env.get('AUTH_TOKEN')
        }

        num_teams  = ((context.config().get('number-of-teams', 4)) as int)
        score_broadcast_interval  = ((context.config().get('score-broadcast-interval', 2500)) as int)
        teams  = Team.createTeams(num_teams)
        
        mechanicsClient  = vertx.createHttpClient()
        achievementClient  = vertx.createHttpClient([
            'maxPoolSize': 100
        ])
        scoreClient  = vertx.createHttpClient(
            ['maxPoolSize': 100]
        )
        setConfiguration(DEFAULT_CONF)
        
        java.lang.Object testPort  = ((context.config().get('innerPort', 9002)) as int)
        
        ( configurationHost ,  configurationPort ,  configurationPath ) = this.retrieveEndpoint('MECHANICS_SERVER', testPort, '/testMechanicsServer')
        this.println('Mechanics Server host: ' +  configurationHost  + ', port ' +  configurationPort  + ', path: ' +  configurationPath )
        
        // BURR
        // (achievementHost, achievementPort, achievementPath) = retrieveEndpoint("ACHIEVEMENTS_SERVER", testPort, "/testAchievementServer");        
        java.lang.Object achievementPortEnv  = System.getenv('ACHIEVEMENTS_SERVER_PORT')?.trim()
        ( achievementHost ,  achievementPort ,  achievementPath ) = this.retrieveEndpoint('ACHIEVEMENTS_SERVER', Integer.parseInt(achievementPortEnv), '/api')
        this.println('Achievement Server host: ' +  achievementHost  + ', port ' +  achievementPort  + ', path: ' +  achievementPath )

        java.lang.Object scorePortEnv  = System.getenv('SCORE_SERVER_PORT')?.trim()
        ( scoreHost ,  scorePort ,  scorePath ) = this.retrieveEndpoint('SCORE_SERVER', Integer.parseInt(scorePortEnv), '/kie-server/services/rest/server/containers/instances/score')
        this.println('Score Server host: ' +  scoreHost  + ', port ' +  scorePort  + ', path: ' +  scorePath )
        
        java.lang.Object scoreUser  = System.getenv('SCORE_USER')?.trim()
        java.lang.Object scorePassword  = System.getenv('SCORE_PASSWORD')?.trim()
        if ( scoreUser  &&  scorePassword ) {
            scoreAuthHeader  = 'Basic ' + Base64.getEncoder().encodeToString((scoreUser + ':' + scorePassword).getBytes('UTF-8'))
        }

        EventBus eventBus  = vertx.eventBus()

        // Receive configuration from the eventbus        
        eventBus.consumer('configuration', { java.lang.Object message ->
            Map configuration  = message.body()
            this.setConfiguration(configuration)
        })
        // A configuration has been pushed, retrieve it from the server.
        eventBus.consumer('configurationUpdated', this.onConfigurationUpdated())
        // New player
        eventBus.consumer('player', this.onNewPlayer(eventBus))
        // New admin
        eventBus.consumer('admin', this.onNewAdmin(eventBus))
        // State change
        eventBus.consumer('state-change', this.onStateChange(eventBus))
        // Selfie state change
        eventBus.consumer('selfie-state-change', this.onSelfieStateChange(eventBus))
        
        // Ask clients to reconnect, used during blue/green deploy
        eventBus.consumer('reconnect', this.onReconnect(eventBus))

        //Ask for scores
        eventBus.consumer('/scores', this.updateTeamScores())

        java.lang.Object futures  = []
        futures.add(this.getTeamCounter())
        futures.addAll(this.getIndividualTeamCounters())
        futures.addAll(this.getIndividualTeamPopCounters())
        futures.add(this.getPlayerNameMap())
        futures.add(this.getGameStateMap())
        
        CompositeFuture.all(futures).setHandler({ java.lang.Object ar ->
            if (ar.succeeded()) {
                future.complete()
            } else {
                ar.cause().printStackTrace()
                future.fail(ar.cause())
            }
        })
        this.retrieveConfiguration()
    }

    // control the state of the game
    // a front-end client with buttons will pass along the current state
    // of the game. states can be: title, demo, play, pause, game-over
    // the updated state will be broadcasted to all connected player clients
    // and to all admin clients so all connections will by in sync. this module,
    // game.js will also hold the state of the game so anytime a new player
    // connects, they will receive the current state of the game in the
    // onPlayerConnection method
    public Handler<Message> onNewAdmin(EventBus eventBus) {
        { java.lang.Object m ->
            Admin admin  = new Admin()
            admins.add(admin)
            java.lang.Object consumer  = eventBus.consumer( admin .userId + '/message')
            consumer.handler({ java.lang.Object msg ->
                Map message  = msg.body()
                java.lang.Object messageEvent  =  message  [ 'event']
                if ( messageEvent  == 'gone') {
                    consumer.unregister()
                    admins.remove(admin)
                    if ( admin .ping != -1) {
                        vertx.cancelTimer( admin .ping)
                    }
                } else {
                    if ( messageEvent  == 'init') {
                        this.send(admin, ['type': 'configuration', 'configuration':  adminConfiguration ])
                        this.send(admin, ['type': 'state', 'state':  state ])
                        this.send(admin, ['type': 'selfie-state', 'state':  selfieState ])
                        admin .ping = vertx.setPeriodic(10000, { java.lang.Object l ->
                            this.send(admin, ['type': 'heartbeat'])
                        })
                    } else {
                        Map data  =  message  [ 'message']
                        if (!( data .token) ||  data .token !=  authToken ) {
                            this.send(admin, ['type': 'auth-failed'])
                        } else {
                            if ( data .type == 'state-change') {
                                if ( data .state == 'start-game') {
                                    java.lang.Object future  = this.resetAll()
                                    future.setHandler({ java.lang.Object ar ->
                                        if (ar.succeeded()) {
                                            this.publish('state-change', data)
                                            data .state = 'play'
                                            this.publish('state-change', data)
                                        } else {
                                            ar.cause().printStackTrace()
                                        }
                                    })
                                } else {
                                    this.publish('state-change', data)
                                }
                            } else {
                                if ( data .type == 'selfie-state-change') {
                                    this.publish('selfie-state-change', data)
                                } else {
                                    if ( data .type == 'configuration') {
                                        this.publish('configuration',  data .configuration)
                                    }
                                }
                            }
                        }
                    }
                }
            }).completionHandler({ java.lang.Object x ->
                m.reply( admin .userId)
            })
        }
    }

    private Handler<Message> onNewPlayer(java.lang.Object eventBus) {
        { java.lang.Object m ->
            java.lang.Object message  = m.body() [ 'message']
            String id  =  message .id
            if ( id ) {
                playerNames.get(id, { java.lang.Object ar ->
                    if (ar.succeeded()) {
                        String name  = ar.result()
                        this.initializePlayerTeam(id, name, m, eventBus)
                    } else {
                        this.initializePlayerTeam(null, null, m, eventBus)
                    }
                })
            } else {
                this.initializePlayerTeam(null, null, m, eventBus)
            }
        }
    }

    private void initializePlayerTeam(String id, String name, Message<Map> m, EventBus eventBus) {
        java.lang.Object message  = m.body() [ 'message']
        Team team  =  message .team ? teams.get( message .team.toInteger()) : null
        if ( id  &&  name  &&  team ) {
            this.initializePlayer(id, name, team, m, eventBus)
        } else {
            counter.addAndGet(1, { java.lang.Object nar ->
                java.lang.Integer assignment  = nar.result() %  num_teams  + 1
                team  = teams.get(assignment)
                this.initializePlayer(id, name, team, m, eventBus)
            })
        }
    }

    private void initializePlayer(String id, String name, Team team, Message<Map> m, EventBus eventBus) {
        Player player
        if ( id  &&  name ) {
            player  = new Player(id, team, name)
        } else {
            player  = new Player(team)
            playerNames.put( player .userId,  player .username, { java.lang.Object ar ->
                if (ar.failed()) {
                    this.println('Put of player name in async map failed, id ' +  player .userId + ', name ' +  player .username)
                }
            })
        }

        java.lang.Object teamCounter  =  teamCounters  [  team .number]
        if ( teamCounter ) {
            teamCounter.incrementAndGet({ java.lang.Object ar ->
            })
        } else {
            this.println('Unexpected team number: ' +  team .number)
        }

        java.lang.Object consumer  = eventBus.consumer( player .userId + '/message')
        consumer.handler({ java.lang.Object msg ->
            java.lang.Object event  = msg.body() [ 'event']
            if ( event  == 'gone') {
                consumer.unregister()
                team .players.remove(player)
                teamCounter?.decrementAndGet({ java.lang.Object ar ->
                })
            } else {
                if ( event  == 'init') {
                    this.send(player, ['type': 'configuration', 'team':  player .team.number, 'playerId':  player .userId, 'username':  player .username, 'score':  team .score, 'configuration':  team .configuration])
                    this.send(player, ['type': 'state', 'state':  state ])
                    this.send(player, ['type': 'selfie-state', 'state':  selfieState ])


                } else {
                    Map message  = msg.body() [ 'message']
                    if ( message .type == 'score') {
                        java.lang.Integer score  = message.getOrDefault('score', 0)
                        java.lang.Integer consecutive  = message.getOrDefault('consecutive', 0)
                        java.lang.Boolean goldenSnitchPopped  = message.getOrDefault('goldenSnitchPopped', false)
                        this.sendScore(player, score, consecutive, goldenSnitchPopped)
                        teamPopCounters  [  team .number]?.addAndGet(1, { 

                        })


                    } else {
                        this.println('Unknown message type : ' +  message .type + ' / ' +  message )


                    }



                }

            }


        }).completionHandler({ java.lang.Object x ->
            team .players.add(player)
            m.reply( player .userId)

        })


    }

    private Handler<Message> onConfigurationUpdated() {
        { java.lang.Object msg ->
            this.println('Received notification about configuration updates')
            this.retrieveConfiguration()

        }


    }

    public java.lang.Object retrieveConfiguration() {
        this.println('Gonna try to retrieve the configuration')
        mechanicsClient.get(configurationPort, configurationHost, configurationPath, { java.lang.Object resp ->
            resp.exceptionHandler({ java.lang.Object t ->
                t.printStackTrace()

            })
            if (resp.statusCode() == 200) {
                resp.bodyHandler({ java.lang.Object body ->
                    Map configuration  = Json.decodeValue(body.toString(),  Map .class)
                    this.println('Retrieved configuration: ' +  configuration )
                    this.setConfiguration(configuration)

                })


            } else {
                this.println('Received error response from Configuration endpoint')


            }


        }).setTimeout(10000).exceptionHandler({ java.lang.Object t ->
            t.printStackTrace()

        }).end()


    }

    public java.lang.Object sendScore(Player player, int score, int consecutivePops, boolean goldenSnitchPopped) {
        java.lang.Object currentAggregatedScore  =  player .aggregatedScore.get()
        if ( currentAggregatedScore  == null) {
            this.println(' currentAggregatedScore == null ')
            java.lang.Object aggregatedScore  = new AggregatedScore(score, consecutivePops, goldenSnitchPopped)
            if (!( player .aggregatedScore.compareAndSet(currentAggregatedScore, aggregatedScore))) {
                this.sendScore(player, score, consecutivePops, goldenSnitchPopped)


            }



        } else {
            java.lang.Object aggregatedScore  = new AggregatedScore( score  >  currentAggregatedScore .score ?  score  :  currentAggregatedScore .score,  consecutivePops  >  currentAggregatedScore .consecutivePops ?  consecutivePops  :  currentAggregatedScore .consecutivePops,  goldenSnitchPopped  ? true :  currentAggregatedScore .goldenSnitchPopped)
            if (!( player .aggregatedScore.compareAndSet(currentAggregatedScore, aggregatedScore))) {
                this.sendScore(player, score, consecutivePops, goldenSnitchPopped)


            } else {
                if (currentAggregatedScore.isDefaulted()) {
                    this.processSend(player)


                }

            }



        }



    }

    public java.lang.Object processSend(Player player) {
        java.lang.Object aggregatedScore  =  player .aggregatedScore.getAndSet(null)
        this.retryableProcessSend(0, player, aggregatedScore, Future.future())


    }

    public java.lang.Object retryProcessSend(int attempt, Player player, AggregatedScore aggregatedScore, Future future) {
        java.lang.Integer newAttempt  =  attempt  + 1
        if ( newAttempt  > 10) {
            this.println('Number of attempts reached ' +  attempt  + ', cancelling')
            future.complete()


        } else {
            vertx.setTimer(1000, { java.lang.Object x ->
                this.retryableProcessSend(newAttempt, player, aggregatedScore, future)

            })


        }



    }

    public java.lang.Object retryableProcessSend(int attempt, Player player, java.lang.Object aggregatedScore, Future future) {
        String uuid  =  player .userId
        String username  =  player .username
        java.lang.Integer team  =  player .team.number
        java.lang.Object playerUpdate  = '{' + '  "lookup"   : "ScoreSession",' + '  "commands" : [' + '  { "insert" : {' + '      "object" : {"com.redhatkeynote.score.Player":{' + '         "uuid"     : "' +  uuid  + '",' + '         "username" : "' +  username  + '",' + '         "team"     : ' +  team  + ',' + '         "score"    : ' +  aggregatedScore .score + ',' + '         "consecutivePops"     : ' +  aggregatedScore .consecutivePops + ',' + '         "goldenSnitch"     : ' +  aggregatedScore .goldenSnitchPopped + '      }}' + '    }' + '  },' + '  { "insert" : {' + '      "object" : {"com.redhatkeynote.score.AchievementList":{' + '      }},' + '      "out-identifier" : "newAchievements",' + '      "return-object" : true' + '    }' + '  },' + '  {' + '    "fire-all-rules" : {}' + '  } ]' + '}'
        future.setHandler({ java.lang.Object ar ->
            java.lang.Object empty  =  player .aggregatedScore.compareAndSet(null, new AggregatedScore())
            if (!( empty )) {
                this.processSend(player)


            }


        })
        java.lang.Object clientRequest  = scoreClient.post(scorePort, scoreHost, scorePath, { java.lang.Object resp ->
            resp.exceptionHandler({ java.lang.Object t ->
                t.printStackTrace()
                this.retryProcessSend(attempt, player, aggregatedScore, future)

            })
            if (resp.statusCode() == 200) {
                resp.bodyHandler({ java.lang.Object body ->
                    java.lang.Object bodyContents  = body.toString()
                    Map response  = Json.decodeValue(bodyContents,  Map .class)
                    if ( response ?.type == 'SUCCESS') {
                        java.lang.Object results  =  response ?.result?.get('execution-results')?.results
                        if ( results ) {
                            for (java.lang.Object result :  results ) {
                                if ( result .key == 'newAchievements') {
                                    java.lang.Object achievements  =  result .value.get('com.redhatkeynote.score.AchievementList')?.achievements
                                    this.updateAchievements(player, achievements)
                                    this.send(player, ['type': 'achievements', 'achievements':  achievements ])


                                }


                            }



                        }



                    } else {
                        this.println('Unsuccessful, received the following body from the score server: ' +  bodyContents )


                    }

                    future.complete()

                })


            } else {
                this.println('Received error response from Score endpoint: ' + resp.statusMessage())
                this.retryProcessSend(attempt, player, aggregatedScore, future)


            }


        }).putHeader('Accept', 'application/json').putHeader('Content-Type', 'application/json').setTimeout(10000).exceptionHandler({ java.lang.Object t ->
            t.printStackTrace()
            this.retryProcessSend(attempt, player, aggregatedScore, future)

        })
        if ( scoreAuthHeader ) {
            clientRequest.putHeader('Authorization', scoreAuthHeader)


        }

        clientRequest.end(playerUpdate)


    }

    public java.lang.Object deleteScores() {
        Future future  = Future.future()
        java.lang.Object deleteScores  = '{' + '  "lookup"   : "ScoreSession",' + '  "commands" : [' + '  { "insert" : {' + '      "object" : {"com.redhatkeynote.score.DeletePlayers":{' + '      }}' + '    }' + '  },' + '  { "insert" : {' + '      "object" : {"com.redhatkeynote.score.AchievementList":{' + '      }},' + '      "out-identifier" : "newAchievements",' + '      "return-object" : true' + '    }' + '  },' + '  {' + '    "fire-all-rules" : {}' + '  } ]' + '}'
        java.lang.Object clientRequest  = scoreClient.post(scorePort, scoreHost, scorePath, { java.lang.Object resp ->
            resp.exceptionHandler({ java.lang.Object t ->
                t.printStackTrace()
                future.complete()

            })
            if (resp.statusCode() != 200) {
                this.println('Received error response from Score endpoint while deleting: ' + resp.statusMessage())


            }

            future.complete()

        }).putHeader('Accept', 'application/json').putHeader('Content-Type', 'application/json').setTimeout(10000).exceptionHandler({ java.lang.Object t ->
            t.printStackTrace()
            future.complete()

        })
        if ( scoreAuthHeader ) {
            clientRequest.putHeader('Authorization', scoreAuthHeader)


        }

        clientRequest.end(deleteScores)

        return  future 



    }

    public java.lang.Object send(java.lang.Object user, Map message) {
        vertx.eventBus().send( user .userId, message)


    }

    public java.lang.Object send(java.lang.Object user, List message) {
        vertx.eventBus().send( user .userId, message)


    }

    public java.lang.Object publish(String address, Map message) {
        vertx.eventBus().publish(address, message)


    }

    public java.lang.Object publish(String address, List message) {
        vertx.eventBus().publish(address, message)


    }

    private Handler<Message> updateTeamScores() {
        { java.lang.Object m ->
            List scores  = m.body()
            java.lang.Object changed  = false
            scores.each({ java.lang.Object score ->
                java.lang.Object team  = teams.get( score .team)
                if ( team .score !=  score .score) {
                    team .score =  score .score
                    changed  = true


                }


            })
            if ( changed  ||  state  == 'play') {
                this.broadcastTeamScores()


            }


        }


    }

    public java.lang.Object updateAchievements(Player player, List achievements) {
        java.lang.Object currentAggregatedAchievements  =  player .aggregatedAchievements.get()
        if ( currentAggregatedAchievements  == null) {
            java.lang.Object aggregatedAchievements  = new AggregatedAchievements(achievements)
            if (!( player .aggregatedAchievements.compareAndSet(currentAggregatedAchievements, aggregatedAchievements))) {
                this.updateAchievements(player, achievements)


            }



        } else {
            java.lang.Object newAchievements  = []
            newAchievements.addAll(achievements)
            newAchievements.addAll( currentAggregatedAchievements .achievements)
            java.lang.Object aggregatedAchievements  = new AggregatedAchievements(newAchievements)
            if (!( player .aggregatedAchievements.compareAndSet(currentAggregatedAchievements, aggregatedAchievements))) {
                this.updateAchievements(player, achievements)


            } else {
                if (currentAggregatedAchievements.isDefaulted()) {
                    this.processUpdateAchievements(player)


                }

            }



        }



    }

    public java.lang.Object processUpdateAchievements(Player player) {
        java.lang.Object future  = Future.future()
        future.setHandler({ java.lang.Object ar ->
            java.lang.Object empty  =  player .aggregatedAchievements.compareAndSet(null, new AggregatedAchievements())
            if (!( empty )) {
                this.processUpdateAchievements(player)


            }


        })
        String uuid  =  player .userId
        java.lang.Object aggregatedAchievements  =  player .aggregatedAchievements.getAndSet(null)
        java.lang.Object achievements  =  aggregatedAchievements .achievements
        if ( achievements  && achievements.size() > 0) {
            java.lang.Object path  =  achievementPath  + '/achievement/update/' +  uuid 
            LOGGER.info('GameVerticle.groovy Achievement Achieved! ' +  player .username + ' ' + Json.encode(achievements) + ' ' +  uuid )
            LOGGER.finest('Achievemnt host:' +  achievementHost )
            LOGGER.finest('port: ' +  achievementPort )
            LOGGER.finest('path: ' +  path )
            achievementClient.put(achievementPort, achievementHost, path, { java.lang.Object resp ->
                if (resp.statusCode() != 200) {
                    LOGGER.info('GameVerticle.groovy:processUpdateAchievements Received  response from Achievement endpoint: ' + resp.statusMessage())
                    LOGGER.finest(Json.encode(achievements))


                }

                future.complete()

            }).setTimeout(3000).exceptionHandler({ java.lang.Object t ->
                t.printStackTrace()
                future.complete()

            }).putHeader('Content-Type', 'application/json').end(Json.encode(achievements))


        } else {
            future.complete()


        }



    }

    public java.lang.Object resetAchievements() {
        java.lang.Object path  =  achievementPath  + '/reset'
        LOGGER.info('resetAchievements reset path: ' +  path )
        Future future  = Future.future()
        achievementClient.delete(achievementPort, achievementHost, path, { java.lang.Object resp ->
            future.complete()
            if (resp.statusCode() != 204) {
                LOGGER.info('GameVerticle.groovy:resetAchievements Response from Achievement endpoint: ' + resp.statusMessage())


            }


        }).setTimeout(3000).exceptionHandler({ 
            future.fail(t)

        }).putHeader('Content-Type', 'application/json').end()

        return  future 



    }

    public java.lang.Object broadcastTeamScores() {
        teams.each({ java.lang.Object i, java.lang.Object team ->
            java.lang.Object message  = ['type': 'team-score', 'score':  team .score]
            team .players.each({ java.lang.Object player ->
                this.send(player, message)

            })

        })


    }

    public java.lang.Object setConfiguration(Map configuration) {
        teams.each({ java.lang.Object i, java.lang.Object team ->
            team .configuration =  configuration 

        })
        adminConfiguration  =  configuration 
        this.broadcastConfigurationChange()


    }

    public java.lang.Object broadcastConfigurationChange() {
        teams.each({ java.lang.Object i, java.lang.Object team ->
            java.lang.Object configurationMessage  = ['type': 'configuration', 'configuration':  team .configuration]
            this.broadcastTeamMessage(team, configurationMessage)

        })
        java.lang.Object configurationMessage  = ['type': 'configuration', 'configuration':  adminConfiguration ]
        this.broadcastAdminMessage(configurationMessage)


    }

    public Handler<Message> onReconnect(EventBus eventBus) {
        { java.lang.Object m ->
            java.lang.Object data  = m.body()
            if (!( data .message.token) ||  data .message.token !=  authToken ) {
                LOGGER.warning('Incorrect token received')
                LOGGER.warning(data.toString())


            } else {
                java.lang.Object message  = ['type': 'reconnect', 'timestamp': System.currentTimeMillis()]
                LOGGER.info('Broadcast reconnect message')
                this.broadcastAllMessage(message)
                this.broadcastAdminMessage(message)


            }


        }


    }

    public Handler<Message> onStateChange(EventBus eventBus) {
        { java.lang.Object m ->
            java.lang.Object data  = m.body()
            this.setState( data .state)
            clusteredState.put('state',  data .state, { java.lang.Object resPut ->
                LOGGER.info('State ' +  data .state + ' was saved in cluster map')

            })

        }


    }

    public java.lang.Object setState(java.lang.Object _state) {
        state  =  _state 
        java.lang.Object message  = ['type': 'state', 'state':  state ]
        this.broadcastAllMessage(message)
        this.broadcastAdminMessage(message)


    }

    public Handler<Message> onSelfieStateChange(EventBus eventBus) {
        { java.lang.Object m ->
            java.lang.Object data  = m.body()
            this.setSelfieState( data .state)

        }


    }

    public java.lang.Object setSelfieState(java.lang.Object _state) {
        selfieState  =  _state 
        java.lang.Object message  = ['type': 'selfie-state', 'state':  selfieState ]
        this.broadcastAllMessage(message)
        this.broadcastAdminMessage(message)


    }

    public java.lang.Object broadcastAllMessage(Map message) {
        teams.each({ java.lang.Object i, java.lang.Object team ->
            this.broadcastTeamMessage(team, message)

        })


    }

    public java.lang.Object broadcastTeamMessage(Team team, Map message) {
        java.lang.Object trafficPercentage  =  message .configuration?.trafficPercentage
        if ( message .type == 'configuration' &&  trafficPercentage  &&  trafficPercentage  != 100) {
            if ( team .players.size() > 0) {
                java.lang.Long seed  = System.nanoTime()
                Collections.shuffle( team .players, new Random(seed))
                java.lang.Object numPlayers  = ((Math.ceil( team .players.size() *  trafficPercentage  / 100)) as int) - 1
                java.lang.Object playersSubset  = []
                team .players [ (0.. numPlayers )].each({ 
                    playersSubset  <<  it 

                })
                playersSubset.each({ java.lang.Object player ->
                    this.send(player, message)

                })


            }



        } else {
            team .players.each({ java.lang.Object player ->
                this.send(player, message)

            })


        }



    }

    public java.lang.Object broadcastAdminMessage(Map message) {
        admins.each({ java.lang.Object admin ->
            this.send(admin, message)

        })


    }

    public static java.lang.Object retrieveEndpoint(String env, int testPort, String testPath) {
        java.lang.Object endpoint  = GameUtils.retrieveEndpoint(env, testPort, testPath)

        return [ endpoint .host,  endpoint .port,  endpoint .path]



    }

    public java.lang.Object getTeamCounter() {
        java.lang.Object future  = Future.future()
        vertx.sharedData().getCounter(TEAM_COUNTER_NAME, { java.lang.Object ar ->
            counter  = ar.result()
            if (ar.succeeded()) {
                future.complete()


            } else {
                future.fail(ar.cause())


            }


        })

        return  future 



    }

    public java.lang.Object getIndividualTeamCounters() {
        java.lang.Object futures  = []
        for (java.lang.Object team : teams.keySet()) {
            java.lang.Object future  = Future.future()
            futures.add(future)
            java.lang.Object name  =  TEAM_COUNTER_NAME  + '.' +  team 
            java.lang.Object teamNumber  =  team 
            vertx.sharedData().getCounter(name, { java.lang.Object ar ->
                teamCounters  [  teamNumber ] = ar.result()
                if (ar.succeeded()) {
                    future.complete()


                } else {
                    future.fail(ar.cause())


                }


            })

        }


        return  futures 



    }

    public java.lang.Object getIndividualTeamPopCounters() {
        java.lang.Object futures  = []
        for (java.lang.Object team : teams.keySet()) {
            java.lang.Object future  = Future.future()
            futures.add(future)
            java.lang.Object name  =  TEAM_POP_COUNTER_NAME  + '.' +  team 
            java.lang.Object teamNumber  =  team 
            vertx.sharedData().getCounter(name, { java.lang.Object ar ->
                teamPopCounters  [  teamNumber ] = ar.result()
                if (ar.succeeded()) {
                    future.complete()


                } else {
                    future.fail(ar.cause())


                }


            })

        }


        return  futures 



    }

    public java.lang.Object getGameStateMap() {
        java.lang.Object future  = Future.future()
        vertx.sharedData().getClusterWideMap(GAME_STATE_MAP, { java.lang.Object ar ->
            if (ar.succeeded()) {
                clusteredState  = ar.result()
                ar.result().get('state', { java.lang.Object resGet ->
                    if ( resGet .result != null) {
                        LOGGER.info('Updating cluster game state to ' +  resGet .result)
                        if ( state  != resGet.result()) {
                            this.setState(resGet.result())


                        }



                    } else {
                        LOGGER.info('Cluster game state has not been set, using default value of \'title\'')


                    }

                    future.complete()

                })


            } else {
                future.fail(ar.cause())


            }


        })

        return  future 



    }

    public java.lang.Object getPlayerNameMap() {
        java.lang.Object future  = Future.future()
        vertx.sharedData().getClusterWideMap(PLAYER_NAME_MAP, { java.lang.Object ar ->
            playerNames  = ar.result()
            if (ar.succeeded()) {
                future.complete()


            } else {
                future.fail(ar.cause())


            }


        })

        return  future 



    }

    public java.lang.Object resetAll() {
        java.lang.Object futures  = []
        futures.add(this.deleteScores())
        futures.add(this.resetAchievements())
        teamPopCounters.each({ java.lang.Object k, java.lang.Object v ->
            Future future  = Future.future()
            futures.add(future)
            java.lang.Object currentCounter  =  v 
            currentCounter.get({ java.lang.Object nar ->
                currentCounter.addAndGet(-1 * nar.result(), { java.lang.Object inner ->
                    future.complete()

                })

            })

        })
        Future composite  = Future.future()
        CompositeFuture.all(futures).setHandler({ java.lang.Object ar ->
            if (ar.succeeded()) {
                composite.complete()


            } else {
                ar.cause().printStackTrace()
                composite.fail(ar.cause())


            }


        })

        return  composite 



    }

}
