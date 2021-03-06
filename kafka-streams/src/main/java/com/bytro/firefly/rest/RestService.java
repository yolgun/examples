package com.bytro.firefly.rest;

import com.bytro.firefly.avro.*;
import com.bytro.firefly.stream.PlanBuilder_v2;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import scala.Tuple2;

import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * A simple REST proxy that runs embedded. This is used to
 * locate and query the State Stores within a Kafka Streams Application.
 */
@Path("state")
public class RestService {
    private KafkaStreams stream;
    private MetadataService metadataService;
    private Server jettyServer;

    @GET()
    public String root() {
        return "{\"message\": \"hello\"}";
    }

    /**
     * Get a key-value pair from a KeyValue Store
     *
     * @param storeName the store to look in
     * @param key       the key to get
     * @return {@link KeyValueBean} representing the key-value pair
     */
    @GET
    @Path("/userscore/{userID}/{scoreType}")
    public String byUserScore(@PathParam("userID") final int userID,
                                          @PathParam("scoreType") final String scoreType) {

        // Lookup the KeyValueStore with the provided storeName
        final ReadOnlyKeyValueStore<UserScore, Value> store =
                stream.store(PlanBuilder_v2.USER_SCORE_STORE, QueryableStoreTypes.keyValueStore());
        if (store == null) {
            throw new NotFoundException();
        }

        // Get the value from the store
        final Value value = store.get(new UserScore(userID, scoreType));
        if (value == null) {
            throw new NotFoundException();
        }
        return "" + value.getValue();
    }

    /**
     * Get a key-value pair from a KeyValue Store
     *
     * @param storeName the store to look in
     * @param key       the key to get
     * @return {@link KeyValueBean} representing the key-value pair
     */
    @GET
    @Path("/userscore/{userID}")
    public String byUserAllScores(@PathParam("userID") final int userID) {

        // Lookup the KeyValueStore with the provided storeName
        final ReadOnlyKeyValueStore<UserScore, Value> store =
                stream.store(PlanBuilder_v2.USER_SCORE_STORE, QueryableStoreTypes.keyValueStore());
        if (store == null) {
            throw new NotFoundException();
        }

        // Get the value from the store
        KeyValueIterator<UserScore, Value> range = store.range(new UserScore(userID, "score-" + 0), new UserScore(userID, "score-" + 10));
        ArrayList<KeyValue> result = new ArrayList<>();
        if (range.hasNext()) {
            for (KeyValue kv = range.next(); range.hasNext(); kv = range.next()) {
                result.add(kv);
            }
        }
        return "" + result;
    }

    /**
     * Get a key-value pair from a KeyValue Store
     *
     * @param storeName the store to look in
     * @param key       the key to get
     * @return {@link KeyValueBean} representing the key-value pair
     */
    @GET
    @Path("/useraward/{userID}")
    public String byUserAllAwards(@PathParam("userID") final int userID) {

        // Lookup the KeyValueStore with the provided storeName
        final ReadOnlyKeyValueStore<UserAward, AwardResult> store =
                stream.store(PlanBuilder_v2.USER_AWARD_STORE, QueryableStoreTypes.keyValueStore());
        if (store == null) {
            throw new NotFoundException();
        }

        // Get the value from the store
        KeyValueIterator<UserAward, AwardResult> range = store.range(new UserAward(userID, 0), new UserAward(userID, 30));
        ArrayList<KeyValue> result = new ArrayList<>();
        if (range.hasNext()) {
            for (KeyValue kv = range.next(); range.hasNext(); kv = range.next()) {
                result.add(kv);
            }
        }
        return "" + result;
    }

    /**
     * Get a key-value pair from a KeyValue Store
     *
     * @param storeName the store to look in
     * @param key       the key to get
     * @return {@link KeyValueBean} representing the key-value pair
     */
    @GET
    @Path("/keyvalue/{storeName}/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public Tuple2<Integer, Integer> byKey(@PathParam("storeName") final String storeName,
                                          @PathParam("key") final int key) {

        // Lookup the KeyValueStore with the provided storeName
        final ReadOnlyKeyValueStore<User, Value> store = stream.store(storeName, QueryableStoreTypes.<User, Value>keyValueStore());
        if (store == null) {
            throw new NotFoundException();
        }

        // Get the value from the store
        final Value value = store.get(new User(key));
        if (value == null) {
            throw new NotFoundException();
        }
        return new Tuple2<>(key, value.getValue());
    }

    /**
     * Get all of the key-value pairs available in a store
     *
     * @param storeName store to query
     * @return A List of {@link KeyValueBean}s representing all of the key-values in the provided
     * store
     */
    @GET()
    @Path("/keyvalues/{storeName}/all")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValueBean> allForStore(@PathParam("storeName") final String storeName) {
        return rangeForKeyValueStore(storeName, ReadOnlyKeyValueStore::all);
    }

    /**
     * Performs a range query on a KeyValue Store and converts the results into a List of
     * {@link KeyValueBean}
     *
     * @param storeName     The store to query
     * @param rangeFunction The range query to run, i.e., all, from(start, end)
     * @return List of {@link KeyValueBean}
     */
    private List<KeyValueBean> rangeForKeyValueStore(final String storeName,
                                                     final Function<ReadOnlyKeyValueStore<String, Long>,
                                                             KeyValueIterator<String, Long>> rangeFunction) {

        // Get the KeyValue Store
        final ReadOnlyKeyValueStore<String, Long> store = stream.store(storeName, QueryableStoreTypes.keyValueStore());
        if (store == null) {
            throw new NotFoundException();
        }

        final List<KeyValueBean> results = new ArrayList<>();
        // Apply the function, i.e., query the store
        final KeyValueIterator<String, Long> range = rangeFunction.apply(store);

        // Convert the results
        while (range.hasNext()) {
            final KeyValue<String, Long> next = range.next();
            results.add(new KeyValueBean(next.key, next.value));
        }

        return results;
    }

    /**
     * Get all of the key-value pairs that have keys within the range from...to
     *
     * @param storeName store to query
     * @param from      start of the range (inclusive)
     * @param to        end of the range (inclusive)
     * @return A List of {@link KeyValueBean}s representing all of the key-values in the provided
     * store that fall withing the given range.
     */
    @GET()
    @Path("/keyvalues/{storeName}/range/{from}/{to}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValueBean> keyRangeForStore(@PathParam("storeName") final String storeName,
                                               @PathParam("from") final String from,
                                               @PathParam("to") final String to) {
        return rangeForKeyValueStore(storeName, store -> store.range(from, to));
    }

    /**
     * Query a window store for key-value pairs representing the value for a provided key within a
     * range of windows
     *
     * @param storeName store to query
     * @param key       key to look for
     * @param from      time of earliest window to query
     * @param to        time of latest window to query
     * @return A List of {@link KeyValueBean}s representing the key-values for the provided key
     * across the provided window range.
     */
    @GET()
    @Path("/windowed/{storeName}/{key}/{from}/{to}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValueBean> windowedByKey(@PathParam("storeName") final String storeName,
                                            @PathParam("key") final String key,
                                            @PathParam("from") final Long from,
                                            @PathParam("to") final Long to) {

        // Lookup the WindowStore with the provided storeName
        final ReadOnlyWindowStore<String, Long> store = stream.store(storeName,
                QueryableStoreTypes.<String, Long>windowStore());
        if (store == null) {
            throw new NotFoundException();
        }

        // fetch the window results for the given key and time range
        final WindowStoreIterator<Long> results = store.fetch(key, from, to);

        final List<KeyValueBean> windowResults = new ArrayList<>();
        while (results.hasNext()) {
            final KeyValue<Long, Long> next = results.next();
            // convert the result to have the window time and the key (for display purposes)
            windowResults.add(new KeyValueBean(key + "@" + next.key, next.value));
        }
        return windowResults;
    }

    /**
     * Get the metadata for all of the instances of this Kafka Streams application
     *
     * @return List of {@link HostStoreInfo}
     */
    @GET()
    @Path("/instances")
    @Produces(MediaType.APPLICATION_JSON)
    public List<HostStoreInfo> streamsMetadata() {
        return metadataService.streamsMetadata();
    }

    /**
     * Get the metadata for all instances of this Kafka Streams application that currently
     * has the provided store.
     *
     * @param store The store to locate
     * @return List of {@link HostStoreInfo}
     */
    @GET()
    @Path("/instances/{storeName}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<HostStoreInfo> streamsMetadataForStore(@PathParam("storeName") String store) {
        return metadataService.streamsMetadataForStore(store);
    }

    /**
     * Find the metadata for the instance of this Kafka Streams Application that has the given
     * store and would have the given key if it exists.
     *
     * @param store Store to find
     * @param key   The key to find
     * @return {@link HostStoreInfo}
     */
    @GET()
    @Path("/instance/{storeName}/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public HostStoreInfo streamsMetadataForStoreAndKey(@PathParam("storeName") String store,
                                                       @PathParam("key") String key) {
        return metadataService.streamsMetadataForStoreAndKey(store, key, new StringSerializer());
    }

    /**
     * Start an embedded Jetty Server on the given port
     *
     * @param port port to run the Server on
     */
    public void start(KafkaStreams stream, final int port) {
        this.stream = stream;
        this.metadataService = new MetadataService(this.stream);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(port);
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        startJetty();
    }

    private void startJetty() {
        try {
            jettyServer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Stop the Jetty Server
     */
    public void stop() {
        try {
            if (jettyServer != null) {
                jettyServer.stop();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

