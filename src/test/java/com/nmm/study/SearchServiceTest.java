package com.nmm.study;

import com.nmm.study.data.ReadData;
import com.nmm.study.search.SearchService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SearchServiceTest {
    @Autowired
    private SearchService searchService;

    @Test
    public void testOne() throws Exception {
        long total = searchService.searchTotal();
        System.out.println("总数：" + total);
    }
    @Test
    public void testTwo() throws IOException {
        String res = searchService.searchTop20Info();
        System.out.println(res);
    }
    @Test
    public void testThree() throws IOException {
        String res = searchService.searchBg360();
        System.out.println(res);
    }
    @Test
    public void testFour() throws IOException {
        String res = searchService.searchAllDay();
        System.out.println(res);
    }
    @Test
    public void testFive() throws IOException {
        String res = searchService.searchFive();
        System.out.println(res);
    }
    @Test
    public void testSix() throws IOException {
        String res = searchService.searchSix();
        System.out.println(res);
    }
    @Test
    public void testSeven() throws IOException {
        String res = searchService.searchSeven();
        System.out.println(res);
    }
}
