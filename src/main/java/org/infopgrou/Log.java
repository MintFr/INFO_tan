package org.infopgrou;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import java.io.File;
import java.util.logging.SimpleFormatter;

public class Log {
    private final Logger logger = Logger.getLogger(Log.class.getName());
    FileHandler fh;

    public Log(String folderName ,String fileName) throws IOException {

        File folder = new File(folderName);
        File f = new File(folderName+fileName);

        if (!folder.exists()) {
            folder.mkdir();
        }

        if (!f.exists()){
            f.createNewFile();
        }


        fh = new FileHandler(folderName+fileName, true);

        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);

    }

    public Logger getLogger() {
        return logger;
    }
}
