package org.onedatashare.transferservice.odstransferservice.config;

import com.google.gson.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Type;
import java.util.Date;

@Configuration
public class SpringConfig {

//    @Bean
//    public Gson gson() {
//
//        return new GsonBuilder().registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {
//            @Override
//            public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
//                return new Date(json.getAsJsonPrimitive().getAsLong());
//            }
//        }).create();
//    }

    @Bean
    public Gson gson(){
        return new GsonBuilder().create();
    }
}
