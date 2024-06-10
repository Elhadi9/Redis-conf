#include <hiredis/hiredis.h>
#include <stdio.h>
#include <stdlib.h>

// Function to connect to Redis
redisContext* connect_to_redis(const char* hostname, int port) {
    redisContext* context = redisConnect(hostname, port);
    if (context == NULL || context->err) {
        if (context) {
            printf("Connection error: %s\n", context->errstr);
            redisFree(context);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }
    return context;
}

// Function to populate Redis with movie data
void populate_redis(redisContext* context) {
    redisReply* reply;

    // Create a movie index
    reply = redisCommand(context, "FT.CREATE movies ON HASH PREFIX 1 movie: SCHEMA title TEXT genre TAG rating NUMERIC");
    if (reply->type == REDIS_REPLY_ERROR) {
        printf("Error creating index: %s\n", reply->str);
    } else {
        printf("Index created successfully\n");
    }
    freeReplyObject(reply);

    // Add some movies
    reply = redisCommand(context, "HMSET movie:1 title 'The Shawshank Redemption' genre 'Drama' rating 9.3");
    freeReplyObject(reply);
    reply = redisCommand(context, "HMSET movie:2 title 'The Godfather' genre 'Crime' rating 9.2");
    freeReplyObject(reply);
    reply = redisCommand(context, "HMSET movie:3 title 'The Dark Knight' genre 'Action' rating 9.0");
    freeReplyObject(reply);
    reply = redisCommand(context, "HMSET movie:4 title 'Pulp Fiction' genre 'Crime' rating 8.9");
    freeReplyObject(reply);

    printf("Movies added successfully\n");
}

int main() {
    // Connect to Redis
    redisContext* context = connect_to_redis("127.0.0.1", 6380);

    // Populate Redis with movie data
    populate_redis(context);

    // Perform an aggregation query to get the average rating of 'Crime' movies
    redisReply* reply = redisCommand(context, 
        "FT.AGGREGATE movies '@genre:{Crime}' GROUPBY 1 @genre REDUCE AVG 1 @rating AS avg_rating");

    if (reply->type == REDIS_REPLY_ARRAY && reply->elements > 1) {
        printf("Average rating of 'Crime' movies: %s\n", reply->element[1]->element[1]->str);
    } else {
        printf("No results found\n");
        if (reply->type == REDIS_REPLY_ERROR) {
            printf("Error: %s\n", reply->str);
        }
    }

    freeReplyObject(reply);

    // Clean up
    redisFree(context);
    return 0;
}
