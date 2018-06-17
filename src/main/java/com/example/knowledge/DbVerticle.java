package com.example.knowledge;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import java.util.function.Function;

public class DbVerticle extends AbstractVerticle {

    private JDBCClient client;

    @Override
    public void start() {
        client = JDBCClient.createNonShared(vertx, new JsonObject()
            .put("url", "jdbc:neo4j:bolt://localhost:7687/")
            .put("user", "neo4j")
            .put("driver_class", "org.neo4j.jdbc.Driver")
            .put("password", "root")
            .put("max_pool_size", 30));

        MessageConsumer<Object> getAllMessageConsumer = vertx.eventBus().consumer("db.getAll");
        getAllMessageConsumer.handler(this::getAllSkills);

        MessageConsumer<Object> getByIdMessageConsumer = vertx.eventBus().consumer("db.getById");
        getByIdMessageConsumer.handler(this::getSkillById);
    }

    private void getAllSkills(Message message) {
        System.out.println("I have received a message: " + message.body());

        client.getConnection(conn -> {
            if (conn.failed()) {
                System.err.println(conn.cause().getMessage());
                return;
            }

            final SQLConnection connection = conn.result();

            connection.query("MATCH (skill:Skill) RETURN {id: ID(skill), name: skill.name}", res -> {
                if (res.succeeded()) {
                    message.reply(res.map(toJsonArray).result());
                } else {
                    throw new RuntimeException(res.cause());
                }
            });
            connection.close(done -> {
                if (done.failed()) {
                    throw new RuntimeException(done.cause());
                }
            });
        });
    }

    private void getSkillById(Message message) {
        Long id = Long.valueOf(message.body().toString());

        client.getConnection(conn -> {
            if (conn.failed()) {
                System.err.println(conn.cause().getMessage());
                return;
            }

            final SQLConnection connection = conn.result();

            connection.queryWithParams("MATCH (skill:Skill) WHERE ID(skill)=? RETURN {id: ID(skill), name: skill.name}",
                new JsonArray().add(id), res -> {
                if (res.succeeded()) {
                    message.reply(res.map(toJsonArray).result());
                } else {
                    throw new RuntimeException(res.cause());
                }
            });
            connection.close(done -> {
                if (done.failed()) {
                    throw new RuntimeException(done.cause());
                }
            });
        });
    }

    private Function<ResultSet,JsonArray> toJsonArray = (resultSet -> {
        JsonArray jsonArray = new JsonArray();
        resultSet.getRows().forEach(jsonArray::add);
        return jsonArray;
    });


}
