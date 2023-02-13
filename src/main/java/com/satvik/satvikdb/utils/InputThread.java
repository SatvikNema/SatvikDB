package com.satvik.satvikdb.utils;

import com.satvik.satvikdb.service.DbService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class InputThread extends Thread {
    private DbService dbService;

    public void setDbService(DbService dbService) {
        this.dbService = dbService;
    }

    @Override
    public void run() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String input, token, key, value;
            while (true) {
                input = br.readLine();

                if(input.startsWith("set")){
                    key = br.readLine();
                    value = br.readLine();
                    System.out.println("setting "+key+" to "+value);
                    dbService.write(key, value);
                } else if(input.startsWith("get")){
                    key = br.readLine();
                    System.out.println("getting for "+key);
                    System.out.println(dbService.read(key));
                }else if("exit".equals(input)){
                    break;
                } else {
                    System.out.println("wrong input enetered. please try again");
                }
            }
        }catch(IOException e){
            System.out.println("IO exception while reading input. "+e.getMessage());
        }
    }
}
