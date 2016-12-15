package io.github.retz.mesosc;

import feign.Param;
import feign.RequestLine;
import io.github.retz.protocol.data.DirEntry;
import io.github.retz.protocol.data.FileContent;

import java.util.List;

/**
 * Created by kuenishi on 16/12/15.
 */
public interface MesosMaster {

    @RequestLine("GET /files/read?path={path}&offset={offset}&length={length}")
    FileContent read(@Param("path") String path,
                     @Param("offset") int offset,
                     @Param("length") int length);

    @RequestLine("GET /files/browse?path={path}")
    List<DirEntry> browse(@Param("path") String path);

    @RequestLine("HEAD /files/read?path={path}&offset={offset}&length={length}")
    FileContent stat(@Param("path") String path,
                     @Param("offset") int offset,
                     @Param("length") int length);
}
