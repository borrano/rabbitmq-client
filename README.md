# STM based rabbitmq client - (not completed)

- Wire protocol, general structure and tests are from: https://hackage.haskell.org/package/amqp  
- Thanks to software transactional memory the implementation is very simple. Details of ampq protocol can be undastood easily inspecting the code. 
 
## TODO:

- add benchmarks. What is the overhead of using STM?
- profile: i expect there are many memory leaks
- if its performance is not absymal fix the API and release