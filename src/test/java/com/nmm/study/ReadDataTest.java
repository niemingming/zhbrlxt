package com.nmm.study;

import com.nmm.study.data.ReadData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ReadDataTest {
    @Autowired
    private ReadData readData;

    @Test
    public void testReadData() throws Exception {
        readData.loadData();
    }
}
