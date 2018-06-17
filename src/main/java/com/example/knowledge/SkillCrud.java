package com.example.knowledge;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class SkillCrud extends AbstractVerticle {

    @Override
    public void start() {
        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());
        router.get("/skills/:id").handler(this::handleGetSkill);
//        router.put("/skills/:id").handler(this::handleAddSkill);
        router.get("/skills").handler(this::handleListSkills);

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
    }

    private void handleGetSkill(RoutingContext routingContext) {
        String skillId = routingContext.request().getParam("id");
        HttpServerResponse response = routingContext.response();

        if (skillId == null) {
            sendError(400, response);
        } else {
            vertx.eventBus().send("db.getById", skillId, reply -> {
                if (reply.succeeded()) {
                    routingContext.response()
                        .putHeader("content-type", "application/json")
                        .end(((JsonArray) reply.result().body()).encodePrettily());
                }
            });
        }
    }

//    private void handleAddSkill(RoutingContext routingContext) {
//        String skillId = routingContext.request().getParam("id");
//        HttpServerResponse response = routingContext.response();
//
//        if (skillId == null) {
//            sendError(400, response);
//        } else {
//            JsonObject skill = routingContext.getBodyAsJson();
//            if (skill == null) {
//                sendError(400, response);
//            } else {
//                skills.put(skillId, skill);
//                response.end();
//            }
//        }
//    }

    private void handleListSkills(RoutingContext routingContext) {
        vertx.eventBus().send("db.getAll", "Give me all skills", reply -> {
            if (reply.succeeded()) {
                routingContext.response()
                    .putHeader("content-type", "application/json")
                    .end(((JsonArray) reply.result().body()).encodePrettily());
            }
        });
    }

    private void sendError(int statusCode, HttpServerResponse response) {
        response.setStatusCode(statusCode).end();
    }

}
