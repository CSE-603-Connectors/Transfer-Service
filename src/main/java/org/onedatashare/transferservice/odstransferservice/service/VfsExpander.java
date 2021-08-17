package org.onedatashare.transferservice.odstransferservice.service;

import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

@Component
public class VfsExpander {

    @SneakyThrows
    public List<EntityInfo> expandListOfFiles(List<EntityInfo> infoList, String basePathToOpen) {
        List<EntityInfo> expandedFiles = new ArrayList<>();
        Stack<Path> traversalStack = new Stack<>();
        Path basePaths = Paths.get(basePathToOpen);
        if (infoList.isEmpty()) { //we want to move everything from the BasePath
            Files.list(basePaths).forEach(path -> {
                if (Files.isDirectory(path)) traversalStack.add(path);
                if (Files.isRegularFile(path)) {
                    try {
                        expandedFiles.add(pathToInfo(path));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        } else {
            for (EntityInfo selectedResource : infoList) {
                Path relative = basePaths.resolve(selectedResource.getPath());
                if (Files.exists(relative)) {
                    if (Files.isDirectory(relative)) {
                        traversalStack.add(relative);
                    } else if (Files.isRegularFile(relative)) {
                        expandedFiles.add(pathToInfo(relative));
                    }
                }
            }
        }
        while (!traversalStack.isEmpty()) {
            Path poppedPath = traversalStack.pop();
            Files.list(poppedPath).forEach(path -> {
                if (Files.isDirectory(path)) {
                    traversalStack.add(path);
                } else if (Files.isRegularFile(path)) {
                    try {
                        expandedFiles.add(pathToInfo(path));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        return expandedFiles;
    }

    public EntityInfo pathToInfo(Path path) throws IOException {
        EntityInfo fileInfo = new EntityInfo();
        fileInfo.setPath(path.toString());
        fileInfo.setId(path.getFileName().toString());
        fileInfo.setSize(Files.size(path));
        return fileInfo;
    }
}
