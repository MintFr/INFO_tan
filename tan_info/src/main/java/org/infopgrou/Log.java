package org.infopgrou;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import java.io.File;
import java.util.logging.SimpleFormatter;

public class Log {
    private final Logger logger = Logger.getLogger(Log.class.getName());
    FileHandler fh;

    public Log(String fileName) throws IOException {

        File f = new File(fileName);

        if (!f.exists()){
            f.createNewFile();
        }

        fh = new FileHandler(fileName, true);

        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);

    }

    public Logger getLogger() {
        return logger;
    }
}
